package walrus

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sort"
	"sync"

	"github.com/couchbase/sg-bucket"
)

// A single view stored in a lolrus.
type lolrusView struct {
	mapFunction         *sgbucket.JSMapFunction // The compiled map function
	reduceFunction      string                  // The source of the reduce function (if any)
	index               sgbucket.ViewResult     // The latest complete result
	lastIndexedSequence uint64                  // Bucket's lastSeq at the time the index was built
}

// Stores view functions for use by a lolrus.
type lolrusDesignDoc map[string]*lolrusView

func (bucket *lolrus) GetDDoc(docname string, into interface{}) error {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	design := bucket.DesignDocs[docname]
	if design == nil {
		return sgbucket.MissingError{docname}
	}
	// Have to roundtrip thru JSON to return it as arbitrary interface{}:
	raw, _ := json.Marshal(design)
	return json.Unmarshal(raw, into)
}

func (bucket *lolrus) PutDDoc(docname string, value interface{}) error {
	design, err := CheckDDoc(value)
	if err != nil {
		return err
	}

	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	if reflect.DeepEqual(design, bucket.DesignDocs[docname]) {
		return nil // unchanged
	}

	err = bucket._compileDesignDoc(docname, design)
	if err != nil {
		return err
	}

	bucket.DesignDocs[docname] = design
	bucket._saveSoon()
	return nil
}

func (bucket *lolrus) DeleteDDoc(docname string) error {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	if bucket.DesignDocs[docname] == nil {
		return sgbucket.MissingError{docname}
	}
	delete(bucket.DesignDocs, docname)
	delete(bucket.views, docname)
	return nil
}

func (bucket *lolrus) _compileDesignDoc(docname string, design *DesignDoc) error {
	if design == nil {
		return nil
	}
	ddoc := lolrusDesignDoc{}
	for name, fns := range design.Views {
		jsserver := sgbucket.NewJSMapFunction(fns.Map)
		view := &lolrusView{
			mapFunction:    jsserver,
			reduceFunction: fns.Reduce,
		}
		ddoc[name] = view
	}
	bucket.views[docname] = ddoc
	return nil
}

// Validates a design document.
func CheckDDoc(value interface{}) (*DesignDoc, error) {
	source, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	var design DesignDoc
	if err := json.Unmarshal(source, &design); err != nil {
		return nil, err
	}

	if design.Language != "" && design.Language != "javascript" {
		return nil, fmt.Errorf("Lolrus design docs don't support language %q",
			design.Language)
	}

	return &design, nil
}

// Looks up a lolrusView, and its current index if it's up-to-date enough.
func (bucket *lolrus) findView(docName, viewName string, staleOK bool) (view *lolrusView, result *sgbucket.ViewResult) {
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	if ddoc, exists := bucket.views[docName]; exists {
		view = ddoc[viewName]
		if view != nil {
			upToDate := view.lastIndexedSequence == bucket.LastSeq
			if !upToDate && view.lastIndexedSequence > 0 && staleOK {
				go bucket.updateView(view, bucket.LastSeq)
				upToDate = true
			}
			if upToDate {
				curResult := view.index // copy the struct
				result = &curResult
			}
		}
	}
	return
}

func (bucket *lolrus) View(docName, viewName string, params map[string]interface{}) (sgbucket.ViewResult, error) {
	// Note: This method itself doesn't lock, so it shouldn't access bucket fields directly.
	log.Printf("View(%q, %q) ...", docName, viewName)

	stale := true
	if params != nil {
		if staleParam, found := params["stale"].(bool); found {
			stale = staleParam
		}
	}

	// Look up the view and its index:
	var result sgbucket.ViewResult
	view, resultMaybe := bucket.findView(docName, viewName, stale)
	if view == nil {
		return result, bucket.missingError(docName + "/" + viewName)
	} else if resultMaybe != nil {
		result = *resultMaybe
	} else {
		result = bucket.updateView(view, 0)
	}

	return sgbucket.ProcessViewResult(result, params, bucket, view.reduceFunction)
}

// Updates the view index if necessary, and returns it.
func (bucket *lolrus) updateView(view *lolrusView, toSequence uint64) sgbucket.ViewResult {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	if toSequence == 0 {
		toSequence = bucket.LastSeq
	}
	if view.lastIndexedSequence >= toSequence {
		return view.index
	}
	log.Printf("\t... updating index to seq %d (from %d)", toSequence, view.lastIndexedSequence)

	var result sgbucket.ViewResult
	result.Rows = make([]*sgbucket.ViewRow, 0, len(bucket.Docs))
	result.Errors = make([]sgbucket.ViewError, 0)

	updatedKeysSize := toSequence - view.lastIndexedSequence
	if updatedKeysSize > 1000 {
		updatedKeysSize = 1000
	}
	updatedKeys := make(map[string]struct{}, updatedKeysSize)

	// Build a parallel task to map docs:
	mapFunction := view.mapFunction
	mapper := func(rawInput interface{}, output chan<- interface{}) {
		input := rawInput.([2]string)
		docid := input[0]
		raw := input[1]
		rows, err := mapFunction.CallFunction(string(raw), docid)
		if err != nil {
			log.Printf("Error running map function: %s", err)
			output <- sgbucket.ViewError{docid, err.Error()}
		} else {
			output <- rows
		}
	}
	mapInput := make(chan interface{})
	mapOutput := Parallelize(mapper, 0, mapInput)

	// Start another task to read the map output and store it into result.Rows/Errors:
	var waiter sync.WaitGroup
	waiter.Add(1)
	go func() {
		defer waiter.Done()
		for item := range mapOutput {
			switch item := item.(type) {
			case sgbucket.ViewError:
				result.Errors = append(result.Errors, item)
			case []*sgbucket.ViewRow:
				result.Rows = append(result.Rows, item...)
			}
		}
	}()

	// Now shovel all the changed document bodies into the mapper:
	for docid, doc := range bucket.Docs {
		if doc.Sequence > view.lastIndexedSequence {
			raw := doc.Raw
			if raw != nil {
				if !doc.IsJSON {
					raw = []byte(`{}`) // Ignore contents of non-JSON (raw) docs
				}
				mapInput <- [2]string{docid, string(raw)}
				updatedKeys[docid] = struct{}{}
			}
		}
	}
	close(mapInput)

	// Wait for the result processing to finish:
	waiter.Wait()

	// Copy existing view rows emitted by unchanged docs:
	for _, row := range view.index.Rows {
		if _, found := updatedKeys[row.ID]; !found {
			result.Rows = append(result.Rows, row)
		}
	}
	for _, err := range view.index.Errors {
		if _, found := updatedKeys[err.From]; !found {
			result.Errors = append(result.Errors, err)
		}
	}

	sort.Sort(&result)
	result.Collator.Clear() // don't keep collation state around

	view.lastIndexedSequence = bucket.LastSeq
	view.index = result
	return view.index
}

func (bucket *lolrus) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	result, err := bucket.View(ddoc, name, params)
	if err != nil {
		return err
	}
	marshaled, _ := json.Marshal(result)
	return json.Unmarshal(marshaled, vres)
}

//////// DUMP:

func (bucket *lolrus) _sortedKeys() []string {
	keys := make([]string, 0, len(bucket.Docs))
	for key, _ := range bucket.Docs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (bucket *lolrus) Dump() {
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()
	fmt.Printf("==== Walrus bucket %q\n", bucket.name)
	for _, key := range bucket._sortedKeys() {
		doc := bucket.Docs[key]
		fmt.Printf("   %q = ", key)
		if doc.IsJSON {
			fmt.Println(string(doc.Raw))
		} else {
			fmt.Printf("<%d bytes>\n", len(doc.Raw))
		}
	}
	fmt.Printf("==== End bucket %q\n", bucket.name)
}
