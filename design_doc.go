//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package walrus

type ViewDef struct {
	Map    string `json:"map"`
	Reduce string `json:"reduce,omitempty"`
}

type ViewMap map[string]ViewDef

type DesignDocOptions struct {
	LocalSeq      bool `json:"local_seq,omitempty"`
	IncludeDesign bool `json:"include_design,omitempty"`
}

// A Couchbase design document, which stores map/reduce function definitions.
type DesignDoc struct {
	Language string            `json:"language,omitempty"`
	Views    ViewMap           `json:"views,omitempty"`
	Options  *DesignDocOptions `json:"options,omitempty"`
}
