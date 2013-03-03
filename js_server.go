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
	"errors"
	"fmt"
	"log"
	"strconv"

	"github.com/robertkrimen/otto"
)

// Go interface to a JavaScript function (like a map/reduce/channelmap/validation function.)
// Each JSServer object compiles a single function into a JavaScript runtime, and lets you
// make thread-safe calls to that function.
type JSServer struct {
	js       *otto.Otto
	fn       otto.Value
	fnSource string

	// Optional function that will be called just before the JS function.
	Before func()

	// Optional function that will be called after the JS function returns, and can convert
	// its output from JS (Otto) values to Go values.
	After func(otto.Value, error) (interface{}, error)

	requests chan jsServerRequest
}

// Creates a new JSServer that will run a JavaScript function.
// 'funcSource' should look like "function(x,y) { ... }"
func NewJSServer(funcSource string) (*JSServer, error) {
	server := &JSServer{js: otto.New(), fn: otto.UndefinedValue()}

	server.DefineNativeFunction("log", func(call otto.FunctionCall) otto.Value {
		var output string
		for _, arg := range call.ArgumentList {
			str, _ := arg.ToString()
			output += str + " "
		}
		log.Printf("JS: %s", output)
		return otto.UndefinedValue()
	})

	if _, err := server.setFunction(funcSource); err != nil {
		return nil, err
	}

	server.requests = make(chan jsServerRequest)
	go server.serve()

	return server, nil
}

func (server *JSServer) setFunction(funcSource string) (bool, error) {
	if funcSource == server.fnSource {
		return false, nil // no-op
	}
	if funcSource == "" {
		server.fn = otto.UndefinedValue()
	} else {
		fnobj, err := server.js.Object("(" + funcSource + ")")
		if err != nil {
			return false, err
		}
		if fnobj.Class() != "Function" {
			return false, errors.New("JavaScript source does not evaluate to a function")
		}
		server.fn = fnobj.Value()
	}
	server.fnSource = funcSource
	return true, nil
}

// Lets you define native helper functions (for example, the "emit" function to be called by
// JS map functions) in the main namespace of the JS runtime.
// This method is not thread-safe and should only be called before making any calls to the
// main JS function.
func (server *JSServer) DefineNativeFunction(name string, function func(otto.FunctionCall) otto.Value) {
	server.js.Set(name, function)
}

// Invokes the JS function. Not thread-safe! This is exposed for use by unit tests.
func (server *JSServer) DirectCallFunction(inputs []string) (interface{}, error) {
	if server.Before != nil {
		server.Before()
	}

	var result otto.Value
	var err error
	if server.fn.IsUndefined() {
		result = otto.UndefinedValue()
	} else {
		inputJS := make([]interface{}, len(inputs))
		for i, inputStr := range inputs {
			if inputStr == "" {
				inputJS[i] = otto.NullValue()
			} else {
				var err error
				inputJS[i], err = server.js.Object("x = " + inputStr)
				if err != nil {
					return nil, fmt.Errorf("Unparseable input %q: %s", inputStr, err)
				}
			}
		}
		result, err = server.fn.Call(server.fn, inputJS...)
	}
	if server.After != nil {
		return server.After(result, err)
	}
	return nil, err
}

//////// SERVER:

const (
	kCallFunction = iota
	kSetFunction
)

type jsServerRequest struct {
	mode          int
	input         []string
	returnAddress chan<- jsServerResponse
}

type jsServerResponse struct {
	output interface{}
	err    error
}

func (server *JSServer) serve() {
	for request := range server.requests {
		var response jsServerResponse
		switch request.mode {
		case kCallFunction:
			response.output, response.err = server.DirectCallFunction(request.input)
		case kSetFunction:
			var changed bool
			changed, response.err = server.setFunction(request.input[0])
			if changed {
				response.output = []string{}
			}
		}
		request.returnAddress <- response
	}
}

func (server *JSServer) request(mode int, input []string) jsServerResponse {
	responseChan := make(chan jsServerResponse, 1)
	server.requests <- jsServerRequest{mode, input, responseChan}
	return <-responseChan
}

// Public thread-safe entry point for invoking the JS function.
// The input is an array of JavaScript expressions (most likely JSON) that will be parsed and
// passed as parameters to the function.
// The output value will be nil unless a custom 'After' function has been installed, in which
// case it'll be the result of that function.
func (server *JSServer) CallFunction(input []string) (interface{}, error) {
	response := server.request(kCallFunction, input)
	return response.output, response.err
}

// Public thread-safe entry point for changing the JS function.
func (server *JSServer) SetFunction(fnSource string) (bool, error) {
	response := server.request(kSetFunction, []string{fnSource})
	return (response.output != nil), response.err
}

// Stops the JS server.
func (server *JSServer) Stop() {
	close(server.requests)
	server.requests = nil
}

// Converts an Otto value to a Go value. Handles all JSON-compatible types.
func OttoToGo(value otto.Value) (interface{}, error) {
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
			return OttoToGoArray(value.Object())
		}
	}
	return nil, fmt.Errorf("Unsupported Otto value: %v", value)
}

func OttoToGoArray(array *otto.Object) ([]interface{}, error) {
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
		result[i], err = OttoToGo(item)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
