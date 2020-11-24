package walrus

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sort"
	"sync"

	sgbucket "github.com/couchbase/sg-bucket"
)

// A single view stored in a Bucket.
type walrusView struct {
	mapFunction         *sgbucket.JSMapFunction // The compiled map function
	reduceFunction      string                  // The source of the reduce function (if any)
	index               sgbucket.ViewResult     // The latest complete result
	lastIndexedSequence uint64                  // Bucket's lastSeq at the time the index was built
}

// Stores view functions for use by a Bucket.
type walrusDesignDoc map[string]*walrusView

func (bucket *WalrusBucket) GetDDocs() (ddocs map[string]sgbucket.DesignDoc, err error) {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	designs := bucket.DesignDocs

	// Roundtrip thru JSON to return it as mutable copy:
	raw, _ := json.Marshal(designs)
	err = json.Unmarshal(raw, &ddocs)
	return ddocs, err
}

func (bucket *WalrusBucket) GetDDoc(docname string) (ddoc sgbucket.DesignDoc, err error) {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	design := bucket.DesignDocs[docname]
	if design == nil {
		return ddoc, sgbucket.MissingError{docname}
	}

	// Roundtrip thru JSON to return it as mutable copy:
	raw, _ := json.Marshal(design)
	err = json.Unmarshal(raw, &ddoc)

	return ddoc, nil
}

func (bucket *WalrusBucket) PutDDoc(docname string, design *sgbucket.DesignDoc) error {
	err := CheckDDoc(design)
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

func (bucket *WalrusBucket) DeleteDDoc(docname string) error {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	if bucket.DesignDocs[docname] == nil {
		return sgbucket.MissingError{docname}
	}
	delete(bucket.DesignDocs, docname)
	delete(bucket.views, docname)
	return nil
}

func (bucket *WalrusBucket) _compileDesignDoc(docname string, design *sgbucket.DesignDoc) error {
	if design == nil {
		return nil
	}
	ddoc := walrusDesignDoc{}
	for name, fns := range design.Views {
		jsserver := sgbucket.NewJSMapFunction(fns.Map)
		view := &walrusView{
			mapFunction:    jsserver,
			reduceFunction: fns.Reduce,
		}
		ddoc[name] = view
	}
	bucket.views[docname] = ddoc
	return nil
}

// Validates a design document.
func CheckDDoc(design *sgbucket.DesignDoc) error {
	if design.Language != "" && design.Language != "javascript" {
		return fmt.Errorf("Walrus design docs don't support language %q",
			design.Language)
	}
	return nil
}

// Looks up a walrusView, and its current index if it's up-to-date enough.
func (bucket *WalrusBucket) findView(docName, viewName string, staleOK bool) (view *walrusView, result *sgbucket.ViewResult) {
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

func (bucket *WalrusBucket) View(docName, viewName string, params map[string]interface{}) (sgbucket.ViewResult, error) {
	// Note: This method itself doesn't lock, so it shouldn't access bucket fields directly.
	logg("View(%q, %q) ...", docName, viewName)

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

func (bucket *WalrusBucket) ViewQuery(ddoc, name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	viewResult, err := bucket.View(ddoc, name, params)
	return &viewResult, err
}

type jsMapFunctionInput struct {
	docid string
	raw   string
	vbNo  uint32
	vbSeq uint64
}

// Updates the view index if necessary, and returns it.
func (bucket *WalrusBucket) updateView(view *walrusView, toSequence uint64) sgbucket.ViewResult {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	if toSequence == 0 {
		toSequence = bucket.LastSeq
	}
	if view.lastIndexedSequence >= toSequence {
		return view.index
	}
	logg("\t... updating index to seq %d (from %d)", toSequence, view.lastIndexedSequence)

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
	mapper := func(input jsMapFunctionInput, output chan<- interface{}) {
		rows, err := mapFunction.CallFunction(
			string(input.raw),
			input.docid,
			input.vbNo,
			input.vbSeq,
		)
		if err != nil {
			log.Printf("Error running map function: %s", err)
			output <- sgbucket.ViewError{input.docid, err.Error()}
		} else {
			output <- rows
		}
	}
	mapInput := make(chan jsMapFunctionInput)
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
				mapInput <- jsMapFunctionInput{
					docid: docid,
					raw:   string(raw),
					vbNo:  doc.VbNo,
					vbSeq: doc.VbSeq,
				}
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

func (bucket *WalrusBucket) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	result, err := bucket.View(ddoc, name, params)
	if err != nil {
		return err
	}
	marshaled, _ := json.Marshal(result)
	return json.Unmarshal(marshaled, vres)
}

func (bucket *WalrusBucket) GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error) {

	return nil, nil, fmt.Errorf("Walrus does not implement GetStatsVbSeqno")

}

//////// DUMP:

func (bucket *WalrusBucket) _sortedKeys() []string {
	keys := make([]string, 0, len(bucket.Docs))
	for key, _ := range bucket.Docs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (bucket *WalrusBucket) Dump() {
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
