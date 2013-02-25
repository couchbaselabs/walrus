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
	"strconv"

	"github.com/robertkrimen/otto"
)

// A compiled JavaScript 'map' function.
type JSMapFunction struct {
	output []ViewRow
	js     *JSServer
}

// Converts an Otto value to a Go value. Handles all JSON-compatible types.
func ottoToGo(value otto.Value) (interface{}, error) {
	if value.IsBoolean() {
		return value.ToBoolean()
	} else if value.IsNull() || value.IsUndefined() {
		return nil, nil
	} else if value.IsNumber() {
		return value.ToFloat()
	} else if value.IsString() {
		return value.ToString()
	} else {
		switch value.Class() {
		case "Array":
			return ottoToGoArray(value.Object())
		}
	}
	return nil, fmt.Errorf("Unsupported Otto value: %v", value)
}

func ottoToGoArray(array *otto.Object) ([]interface{}, error) {
	lengthVal, err := array.Get("length")
	if err != nil {
		return nil, err
	}
	length, err := lengthVal.ToInteger()
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, length)
	for i := 0; i < int(length); i++ {
		item, err := array.Get(strconv.Itoa(i))
		if err != nil {
			return nil, err
		}
		result[i], err = ottoToGo(item)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
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
		key, err1 := ottoToGo(call.ArgumentList[0])
		value, err2 := ottoToGo(call.ArgumentList[1])
		if err1 != nil || err2 != nil {
			panic(fmt.Sprintf("Unsupported key or value types: key=%v, value=%v", key, value))
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

func makeMeta(docid string) string {
	meta := map[string]interface{}{"id": docid}
	rawMeta, _ := json.Marshal(meta)
	return string(rawMeta)
}

// This is just for testing
func (mapper *JSMapFunction) callMapper(doc string, docid string) ([]ViewRow, error) {
	res, err := mapper.js.DirectCallFunction([]string{doc, makeMeta(docid)})
	return res.([]ViewRow), err
}

// Calls a JSMapFunction. This is thread-safe.
func (mapper *JSMapFunction) CallFunction(doc string, docid string) ([]ViewRow, error) {
	result1, err := mapper.js.CallFunction([]string{doc, makeMeta(docid)})
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
