//  Copyright (c) 2012-2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package walrus

import (
	"encoding/json"
	"fmt"

	"github.com/robertkrimen/otto"
)

// A compiled JavaScript 'map' function.
type JSMapFunction struct {
	output []ViewRow
	js     *JSServer
}

// Compiles a JavaScript map function to a JSMapFunction object.
func NewJSMapFunction(funcSource string) (*JSMapFunction, error) {
	mapper := &JSMapFunction{}
	var err error
	mapper.js, err = NewJSServer(funcSource)
	if err != nil {
		return nil, err
	}

	// Implementation of the 'emit()' callback:
	mapper.js.DefineNativeFunction("emit", func(call otto.FunctionCall) otto.Value {
		key, err1 := call.ArgumentList[0].Export()
		value, err2 := call.ArgumentList[1].Export()
		if err1 != nil || err2 != nil {
			panic(fmt.Sprintf("Unsupported key or value types: emit(%#v,%#v): %v %v", key, value, err1, err2))
		}
		mapper.output = append(mapper.output, ViewRow{Key: key, Value: value})
		return otto.UndefinedValue()
	})

	mapper.js.Before = func() {
		mapper.output = []ViewRow{}
	}
	mapper.js.After = func(result otto.Value, err error) (interface{}, error) {
		output := mapper.output
		mapper.output = nil
		return output, err
	}
	return mapper, nil
}

func MakeMeta(docid string) string {
	meta := map[string]interface{}{"id": docid}
	rawMeta, _ := json.Marshal(meta)
	return string(rawMeta)
}

// This is just for testing
func (mapper *JSMapFunction) callMapper(doc string, docid string) ([]ViewRow, error) {
	res, err := mapper.js.DirectCallFunction([]string{doc, MakeMeta(docid)})
	return res.([]ViewRow), err
}

// Calls a JSMapFunction. This is thread-safe.
func (mapper *JSMapFunction) CallFunction(doc string, docid string) ([]ViewRow, error) {
	result1, err := mapper.js.CallFunction([]string{doc, MakeMeta(docid)})
	if err != nil {
		return nil, err
	}
	rows := result1.([]ViewRow)
	for i, _ := range rows {
		rows[i].ID = docid
	}
	return rows, nil
}

func (mapper *JSMapFunction) SetFunction(fnSource string) (bool, error) {
	return mapper.js.SetFunction(fnSource)
}

func (mapper *JSMapFunction) Stop() {
	mapper.js.Stop()
}
