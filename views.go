package walrus

import (
	"encoding/json"
	"fmt"
	"sort"
)

// A single view stored in a lolrus.
type lolrusView struct {
	mapFunction         *JSMapFunction // The compiled map function
	reduceFunction      string         // The source of the reduce function (if any)
	index               ViewResult     // The latest complete result
	lastIndexedSequence uint64         // Bucket's lastSeq at the time the index was built
}

// Stores view functions for use by a lolrus.
type lolrusDesignDoc map[string]*lolrusView

func (bucket *lolrus) PutDDoc(docname string, value interface{}) error {
	source, err := json.Marshal(value)
	if err != nil {
		return err
	}

	var design DesignDoc
	if err := json.Unmarshal(source, &design); err != nil {
		return err
	}

	if design.Language != "" && design.Language != "javascript" {
		return fmt.Errorf("Lolrus design docs don't support language %q", design.Language)
	}

	ddoc := lolrusDesignDoc{}
	for name, fns := range design.Views {
		jsserver, err := NewJSMapFunction(fns.Map)
		if err != nil {
			return err
		}
		ddoc[name] = &lolrusView{
			mapFunction:    jsserver,
			reduceFunction: fns.Reduce,
		}
	}

	// Now store the design doc in the Bucket:
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	bucket.designDocs[docname] = ddoc
	return nil
}

// Looks up a lolrusView, and its current index if it's up-to-date enough.
func (bucket *lolrus) findView(docName, viewName string, staleOK bool) (view *lolrusView, result *ViewResult) {
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	if ddoc, exists := bucket.designDocs[docName]; exists {
		view = ddoc[viewName]
		if view != nil {
			upToDate := view.lastIndexedSequence == bucket.lastSeq
			if !upToDate && staleOK {
				go bucket.updateView(view, bucket.lastSeq)
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

func (bucket *lolrus) View(docName, viewName string, params map[string]interface{}) (ViewResult, error) {
	// Note: This method itself doesn't lock, so it shouldn't access bucket fields directly.

	// Extract view options:
	var includeDocs bool
	var limit int
	reverse := false
	stale := true
	reduce := true
	if params != nil {
		includeDocs, _ = params["include_docs"].(bool)
		limit, _ = params["limit"].(int)
		reverse, _ = params["reverse"].(bool)
		if staleParam, found := params["stale"].(bool); found {
			stale = staleParam
		}
		if reduceParam, found := params["reduce"].(bool); found {
			reduce = reduceParam
		}
	}

	// Look up the view and its index:
	var result ViewResult
	view, resultMaybe := bucket.findView(docName, viewName, stale)
	if view == nil {
		return result, MissingError{}
	} else if resultMaybe != nil {
		result = *resultMaybe
	} else {
		result = bucket.updateView(view, 0)
	}

	// Apply view options:

	if reverse {
		//TODO: Apply "reverse" option
		return result, fmt.Errorf("Reverse is not supported yet, sorry")
	}

	startkey := params["startkey"]
	if startkey != nil {
		i := sort.Search(len(result.Rows), func(i int) bool {
			return CollateJSON(result.Rows[i].Key, startkey) >= 0
		})
		result.Rows = result.Rows[i:]
	}

	if limit > 0 && len(result.Rows) > limit {
		result.Rows = result.Rows[:limit]
	}

	endkey := params["endkey"]
	if endkey != nil {
		i := sort.Search(len(result.Rows), func(i int) bool {
			return CollateJSON(result.Rows[i].Key, endkey) > 0
		})
		result.Rows = result.Rows[:i]
	}

	if includeDocs {
		newRows := make(ViewRows, len(result.Rows))
		for i, row := range result.Rows {
			//OPT: This may unmarshal the same doc more than once
			raw := bucket.docs[row.ID].raw
			var parsedDoc interface{}
			json.Unmarshal(raw, &parsedDoc)
			newRows[i] = row
			newRows[i].Doc = &parsedDoc
		}
		result.Rows = newRows
	}

	if reduce && view.reduceFunction != "" {
		//TODO: Apply reduce function!!
		return result, fmt.Errorf("Reduce is not supported yet, sorry")
	}

	result.TotalRows = len(result.Rows)
	return result, nil
}

// Updates the view index if necessary, and returns it.
func (bucket *lolrus) updateView(view *lolrusView, toSequence uint64) ViewResult {
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	if toSequence == 0 {
		toSequence = bucket.lastSeq
	}
	if view.lastIndexedSequence >= toSequence {
		return view.index
	}

	//OPT: Should index incrementally by re-mapping rows of docs whose sequence > lastIndexedSequence
	var result ViewResult
	result.Rows = make([]ViewRow, 0, len(bucket.docs))
	result.Errors = make([]ViewError, 0)
	for docid, doc := range bucket.docs {
		raw := doc.raw
		if raw == nil {
			continue
		}
		if !doc.isJSON {
			raw = []byte(`{}`) // Ignore contents of non-JSON (raw) docs
		}
		rows, err := view.mapFunction.CallFunction(string(raw), docid)
		if err != nil {
			result.Errors = append(result.Errors, ViewError{docid, err.Error()})
		} else {
			result.Rows = append(result.Rows, rows...)
		}
	}
	sort.Sort(result.Rows)

	view.lastIndexedSequence = bucket.lastSeq
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
