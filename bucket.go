//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package walrus

import (
	"errors"
	"fmt"
	"log"
)

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
	Write(k string, flags int, exp int, v interface{}, opt WriteOptions) error
	Update(k string, exp int, callback UpdateFunc) error
	WriteUpdate(k string, exp int, callback WriteUpdateFunc) error
	Incr(k string, amt, def uint64, exp int) (uint64, error)
	PutDDoc(docname string, value interface{}) error
	View(ddoc, name string, params map[string]interface{}) (ViewResult, error)
	ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error
	StartTapFeed(args TapArguments) (TapFeed, error)
	Close()
	Dump()
}

// A set of option flags for the Write method.
type WriteOptions int

const (
	Raw       = WriteOptions(1 << iota) // Value is raw []byte; don't JSON-encode it
	AddOnly                             // Fail with ErrKeyExists if key already has a value
	Persist                             // After write, wait until it's written to disk
	Indexable                           // After write, wait until it's ready for views to index
)

// Result of a view query.
type ViewResult struct {
	TotalRows int `json:"total_rows"`
	Rows      ViewRows
	Errors    []ViewError
	collator  JSONCollator
}

type ViewRows []*ViewRow

// A single result row from a view query.
type ViewRow struct {
	ID    string
	Key   interface{}
	Value interface{}
	Doc   *interface{}
}

// Type of error returned by Bucket API when a document is missing
type MissingError struct{ key string }

func (err MissingError) Error() string {
	return fmt.Sprintf("key %q missing", err.key)
}

// Error returned from Write with AddOnly flag, when key already exists in the bucket.
// (This is *not* returned from the Add method! Add has an extra boolean parameter to
// indicate this state, so it returns (false,nil).)
var ErrKeyExists = errors.New("Key exists")

// Error returned from Write with Perist or Indexable flags, if the value doesn't become
// persistent or indexable within the timeout period.
var ErrTimeout = errors.New("Timeout")

type ViewError struct {
	From   string
	Reason string
}

func (ve ViewError) Error() string {
	return fmt.Sprintf("Node: %v, reason: %v", ve.From, ve.Reason)
}

type UpdateFunc func(current []byte) (updated []byte, err error)

type WriteUpdateFunc func(current []byte) (updated []byte, opt WriteOptions, err error)

// Set this to true to enable logging

var Logging bool

func ohai(fmt string, args ...interface{}) {
	if Logging {
		log.Printf("Walrus: "+fmt, args...)
	}
}
