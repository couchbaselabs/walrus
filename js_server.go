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
	"github.com/robertkrimen/otto"
)

const kMaxRunners = 4

// Go interface to a JavaScript function (like a map/reduce/channelmap/validation function.)
// Each JSServer object compiles a single function into a JavaScript runtime, and lets you
// make thread-safe calls to that function.
type JSServer struct {
	fnSource string

	nativeFns map[string]NativeFunction

	// Optional function that will be called just before the JS function.
	Before func()

	// Optional function that will be called after the JS function returns, and can convert
	// its output from JS (Otto) values to Go values.
	After func(otto.Value, error) (interface{}, error)

	runners chan *JSRunner
}

// Creates a new JSServer that will run a JavaScript function.
// 'funcSource' should look like "function(x,y) { ... }"
func NewJSServer(funcSource string) (*JSServer, error) {
	server := &JSServer{
		fnSource:  funcSource,
		nativeFns: map[string]NativeFunction{},
		runners:   make(chan *JSRunner, kMaxRunners),
	}
	return server, nil
}

// Lets you define native helper functions (for example, the "emit" function to be called by
// JS map functions) in the main namespace of the JS runtime.
// This method is not thread-safe and should only be called before making any calls to the
// main JS function.
func (server *JSServer) DefineNativeFunction(name string, function NativeFunction) {
	server.nativeFns[name] = function
}

func (server *JSServer) createRunner() (*JSRunner, error) {
	runner, err := NewJSRunner(server.fnSource)
	if err != nil {
		return nil, err
	}
	runner.Before = server.Before
	runner.After = server.After
	for name, fn := range server.nativeFns {
		runner.DefineNativeFunction(name, fn)
	}
	return runner, err
}

func (server *JSServer) getRunner() (*JSRunner, error) {
	var err error
	select {
	case runner := <-server.runners:
		_, err = runner.SetFunction(server.fnSource)
		return runner, err
	default:
		return server.createRunner()
	}
}

func (server *JSServer) returnRunner(runner *JSRunner) {
	select {
	case server.runners <- runner:
	default:
		// Drop it on the floor if the pool is already full
	}
}

//////// SERVER:

// Public thread-safe entry point for invoking the JS function.
// The input parameters are JavaScript expressions (most likely JSON) that will be parsed and
// passed as parameters to the function.
// The output value will be nil unless a custom 'After' function has been installed, in which
// case it'll be the result of that function.
func (server *JSServer) CallWithJSON(jsonParams ...string) (interface{}, error) {
	runner, err := server.getRunner()
	if err != nil {
		return nil, err
	}
	defer server.returnRunner(runner)
	return runner.CallWithJSON(jsonParams...)
}

// Public thread-safe entry point for invoking the JS function.
// The input parameters are Go values that will be converted to JavaScript values.
// JSON can be passed in as a value of type JSONString (a wrapper type for string.)
// The output value will be nil unless a custom 'After' function has been installed, in which
// case it'll be the result of that function.
func (server *JSServer) Call(goParams ...interface{}) (interface{}, error) {
	runner, err := server.getRunner()
	if err != nil {
		return nil, err
	}
	defer server.returnRunner(runner)
	return runner.Call(goParams...)
}

// Public thread-safe entry point for changing the JS function.
func (server *JSServer) SetFunction(fnSource string) (bool, error) {
	if fnSource == server.fnSource {
		return false, nil
	}
	server.fnSource = fnSource
	return true, nil
}

//////// DEPRECATED API:

// Deprecated synonym for CallWithJSON, kept around for backward compatibility.
func (server *JSServer) CallFunction(jsonParams []string) (interface{}, error) {
	return server.CallWithJSON(jsonParams...)
}

// Stops the JS server. (For backward compatibility only; does nothing)
func (server *JSServer) Stop() {
}
