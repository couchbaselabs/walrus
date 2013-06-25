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

type NativeFunction func(otto.FunctionCall) otto.Value

// Go interface to a JavaScript function (like a map/reduce/channelmap/validation function.)
// Each JSServer object compiles a single function into a JavaScript runtime, and lets you
// call that function.
// JSRunner is NOT thread-safe! For that, use JSServer, a wrapper around it.
type JSRunner struct {
	js       *otto.Otto
	fn       otto.Value
	fnSource string

	// Optional function that will be called just before the JS function.
	Before func()

	// Optional function that will be called after the JS function returns, and can convert
	// its output from JS (Otto) values to Go values.
	After func(otto.Value, error) (interface{}, error)
}

// Creates a new JSRunner that will run a JavaScript function.
// 'funcSource' should look like "function(x,y) { ... }"
func NewJSRunner(funcSource string) (*JSRunner, error) {
	runner := &JSRunner{js: otto.New(), fn: otto.UndefinedValue()}

	runner.DefineNativeFunction("log", func(call otto.FunctionCall) otto.Value {
		var output string
		for _, arg := range call.ArgumentList {
			str, _ := arg.ToString()
			output += str + " "
		}
		log.Printf("JS: %s", output)
		return otto.UndefinedValue()
	})

	if _, err := runner.SetFunction(funcSource); err != nil {
		return nil, err
	}

	return runner, nil
}

func (runner *JSRunner) SetFunction(funcSource string) (bool, error) {
	if funcSource == runner.fnSource {
		return false, nil // no-op
	}
	if funcSource == "" {
		runner.fn = otto.UndefinedValue()
	} else {
		fnobj, err := runner.js.Object("(" + funcSource + ")")
		if err != nil {
			return false, err
		}
		if fnobj.Class() != "Function" {
			return false, errors.New("JavaScript source does not evaluate to a function")
		}
		runner.fn = fnobj.Value()
	}
	runner.fnSource = funcSource
	return true, nil
}

// Lets you define native helper functions (for example, the "emit" function to be called by
// JS map functions) in the main namespace of the JS runtime.
// This method is not thread-safe and should only be called before making any calls to the
// main JS function.
func (runner *JSRunner) DefineNativeFunction(name string, function NativeFunction) {
	runner.js.Set(name, (func(otto.FunctionCall) otto.Value)(function))
}

func (runner *JSRunner) jsonToValue(json string) (interface{}, error) {
	if json == "" {
		return otto.NullValue(), nil
	}

	value, err := runner.js.Object("x = " + json)
	if err != nil {
		err = fmt.Errorf("Unparseable input %q: %s", json, err)
	}
	return value, err
}

// Invokes the JS function with JSON inputs.
func (runner *JSRunner) CallWithJSON(inputs ...string) (interface{}, error) {
	if runner.Before != nil {
		runner.Before()
	}

	var result otto.Value
	var err error
	if runner.fn.IsUndefined() {
		result = otto.UndefinedValue()
	} else {
		inputJS := make([]interface{}, len(inputs))
		for i, inputStr := range inputs {
			inputJS[i], err = runner.jsonToValue(inputStr)
			if err != nil {
				return nil, err
			}
		}
		result, err = runner.fn.Call(runner.fn, inputJS...)
	}
	if runner.After != nil {
		return runner.After(result, err)
	}
	return nil, err
}

// Invokes the JS function with Go inputs.
func (runner *JSRunner) Call(inputs ...interface{}) (interface{}, error) {
	if runner.Before != nil {
		runner.Before()
	}

	var result otto.Value
	var err error
	if runner.fn.IsUndefined() {
		result = otto.UndefinedValue()
	} else {
		inputJS := make([]interface{}, len(inputs))
		for i, input := range inputs {
			switch input := input.(type) {
			case JSONString:
				inputJS[i], err = runner.jsonToValue(string(input))
			default:
				inputJS[i], err = runner.js.ToValue(input)
			}
			if err != nil {
				return nil, fmt.Errorf("Couldn't convert %#v to JS: %s", input, err)
			}
		}
		result, err = runner.fn.Call(runner.fn, inputJS...)
	}
	if runner.After != nil {
		return runner.After(result, err)
	}
	return nil, err
}
