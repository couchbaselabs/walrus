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

	"github.com/robertkrimen/otto"
)

// Alternate type to wrap a Go string in to mark that Call() should interpret it as JSON.
// That is, when Call() sees a parameter of type JSONString it will parse the JSON and use
// the result as the parameter value, instead of just converting it to a JS string.
type JSONString string

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
	go server.serve(server.requests)

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

func (server *JSServer) jsonToValue(json string) (interface{}, error) {
	if json == "" {
		return otto.NullValue(), nil
	}

	value, err := server.js.Object("x = " + json)
	if err != nil {
		err = fmt.Errorf("Unparseable input %q: %s", json, err)
	}
	return value, err
}

// Invokes the JS function with JSON inputs. Not thread-safe! This is exposed for use by unit tests.
func (server *JSServer) DirectCallWithJSON(inputs ...string) (interface{}, error) {
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
			inputJS[i], err = server.jsonToValue(inputStr)
			if err != nil {
				return nil, err
			}
		}
		result, err = server.fn.Call(server.fn, inputJS...)
	}
	if server.After != nil {
		return server.After(result, err)
	}
	return nil, err
}

// Invokes the JS function with Go inputs. Not thread-safe! This is exposed for use by unit tests.
func (server *JSServer) DirectCall(inputs ...interface{}) (interface{}, error) {
	if server.Before != nil {
		server.Before()
	}

	var result otto.Value
	var err error
	if server.fn.IsUndefined() {
		result = otto.UndefinedValue()
	} else {
		inputJS := make([]interface{}, len(inputs))
		for i, input := range inputs {
			switch input := input.(type) {
			case JSONString:
				inputJS[i], err = server.jsonToValue(string(input))
			default:
				inputJS[i], err = server.js.ToValue(input)
			}
			if err != nil {
				return nil, fmt.Errorf("Couldn't convert %#v to JS: %s", input, err)
			}
		}
		result, err = server.fn.Call(server.fn, inputJS...)
	}
	if server.After != nil {
		return server.After(result, err)
	}
	return nil, err
}

// Deprecated equivalent of DirectCallWithJSON, kept around for backward compatibility.
func (server *JSServer) DirectCallFunction(inputs []string) (interface{}, error) {
	return server.DirectCallWithJSON(inputs...)
}

//////// SERVER:

const (
	kCallFunction = iota
	kCallFunctionWithJSON
	kSetFunction
)

type jsServerRequest struct {
	mode          int
	input         interface{}
	returnAddress chan<- jsServerResponse
}

type jsServerResponse struct {
	output interface{}
	err    error
}

func (server *JSServer) serve(requests <-chan jsServerRequest) {
	for request := range requests {
		var response jsServerResponse
		switch request.mode {
		case kCallFunction:
			response.output, response.err = server.DirectCall(request.input.([]interface{})...)
		case kCallFunctionWithJSON:
			response.output, response.err = server.DirectCallWithJSON(request.input.([]string)...)
		case kSetFunction:
			var changed bool
			changed, response.err = server.setFunction(request.input.(string))
			if changed {
				response.output = []string{}
			}
		}
		request.returnAddress <- response
	}
}

func (server *JSServer) request(mode int, input interface{}) jsServerResponse {
	responseChan := make(chan jsServerResponse, 1)
	server.requests <- jsServerRequest{mode, input, responseChan}
	return <-responseChan
}

// Public thread-safe entry point for invoking the JS function.
// The input parameters are JavaScript expressions (most likely JSON) that will be parsed and
// passed as parameters to the function.
// The output value will be nil unless a custom 'After' function has been installed, in which
// case it'll be the result of that function.
func (server *JSServer) CallWithJSON(jsonParams ...string) (interface{}, error) {
	response := server.request(kCallFunctionWithJSON, jsonParams)
	return response.output, response.err
}

// Public thread-safe entry point for invoking the JS function.
// The input parameters are Go values that will be converted to JavaScript values.
// JSON can be passed in as a value of type JSONString (a wrapper type for string.)
// The output value will be nil unless a custom 'After' function has been installed, in which
// case it'll be the result of that function.
func (server *JSServer) Call(goParams ...interface{}) (interface{}, error) {
	response := server.request(kCallFunction, goParams)
	return response.output, response.err
}

// Deprecated synonym for CallWithJSON, kept around for backward compatibility.
func (server *JSServer) CallFunction(jsonParams []string) (interface{}, error) {
	return server.CallWithJSON(jsonParams...)
}

// Public thread-safe entry point for changing the JS function.
func (server *JSServer) SetFunction(fnSource string) (bool, error) {
	response := server.request(kSetFunction, fnSource)
	return (response.output != nil), response.err
}

// Stops the JS server.
func (server *JSServer) Stop() {
	close(server.requests)
}
