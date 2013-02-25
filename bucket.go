//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package walrus

import "fmt"

// Abstract storage interface based on Bucket from the go-couchbase package.
// A Bucket is a key-value store with a map/reduce query interface, as found in Couchbase Server 2.
type Bucket interface {
	GetName() string
	Get(k string, rv interface{}) error
	GetRaw(k string) ([]byte, error)
	Add(k string, exp int, v interface{}) (added bool, err error)
	AddRaw(k string, exp int, v []byte) (added bool, err error)
	Set(k string, exp int, v interface{}) error
	SetRaw(k string, exp int, v []byte) error
	Delete(k string) error
	Update(k string, exp int, callback UpdateFunc) error
	Incr(k string, amt, def uint64, exp int) (uint64, error)
	PutDDoc(docname string, value interface{}) error
	View(ddoc, name string, params map[string]interface{}) (ViewResult, error)
	ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error
}

// Result of a view query.
type ViewResult struct {
	TotalRows int `json:"total_rows"`
	Rows      ViewRows
	Errors    []ViewError
}

type ViewRows []ViewRow

// A single result row from a view query.
type ViewRow struct {
	ID    string
	Key   interface{}
	Value interface{}
	Doc   *interface{}
}

// Error returned by Bucket API when a document is missing
type MissingError struct{}

func (err MissingError) Error() string {
	return "missing"
}

type ViewError struct {
	From   string
	Reason string
}

func (ve ViewError) Error() string {
	return fmt.Sprintf("Node: %v, reason: %v", ve.From, ve.Reason)
}

type UpdateFunc func(current []byte) (updated []byte, err error)

//////// VIEW ROWS: (implementation of sort.Interface interface)

func (rows ViewRows) Len() int {
	return len(rows)
}

func (rows ViewRows) Swap(i, j int) {
	temp := rows[i]
	rows[i] = rows[j]
	rows[j] = temp
}

func (rows ViewRows) Less(i, j int) bool {
	return CollateJSON(rows[i].Key, rows[j].Key) < 0
}
