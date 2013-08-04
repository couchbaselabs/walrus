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
	"encoding/json"
	"fmt"

	"code.google.com/p/go.exp/locale/collate"
	"code.google.com/p/go.text/locale"
)

// Context for JSON collation. This struct is not thread-safe (or rather, its embedded string
// collator isn't) so it should only be used on one goroutine at a time.
type JSONCollator struct {
	stringCollator *collate.Collator
}

func defaultLocale() locale.ID {
	l, e := locale.Parse("icu")
	if e != nil {
		return locale.Und
	}
	return l
}

func CollateJSON(key1, key2 interface{}) int {
	var collator JSONCollator
	return collator.Collate(key1, key2)
}

func (c *JSONCollator) Clear() {
	c.stringCollator = nil
}

// CouchDB-compatible collation/comparison of JSON values.
// See: http://wiki.apache.org/couchdb/View_collation#Collation_Specification
func (c *JSONCollator) Collate(key1, key2 interface{}) int {
	type1 := collationType(key1)
	type2 := collationType(key2)
	if type1 != type2 {
		return type1 - type2
	}
	switch type1 {
	case 0, 1, 2:
		return 0
	case 3:
		n1 := collationToFloat64(key1)
		n2 := collationToFloat64(key2)
		if n1 < n2 {
			return -1
		} else if n1 > n2 {
			return 1
		}
		return 0
	case 4:
		s1 := key1.(string)
		s2 := key2.(string)
		stringCollator := c.stringCollator
		if stringCollator == nil {
			stringCollator = collate.New(defaultLocale())
			c.stringCollator = stringCollator
		}
		return stringCollator.CompareString(s1, s2)
	case 5:
		array1 := key1.([]interface{})
		array2 := key2.([]interface{})
		for i, item1 := range array1 {
			if i >= len(array2) {
				return 1
			}
			if cmp := c.Collate(item1, array2[i]); cmp != 0 {
				return cmp
			}
		}
		return len(array1) - len(array2)
	case 6:
		return 0 // ignore ordering for catch-all stuff
	}
	panic("bogus collationType")
}

func collationType(value interface{}) int {
	if value == nil {
		return 0
	}
	switch value := value.(type) {
	case bool:
		if !value {
			return 1
		}
		return 2
	case float64, uint64, json.Number:
		return 3
	case string:
		return 4
	case []interface{}:
		return 5
	case map[string]interface{}:
		return 6
	}
	panic(fmt.Sprintf("collationType doesn't understand %+v (%T)", value, value))
}

func collationToFloat64(value interface{}) float64 {
	if i, ok := value.(uint64); ok {
		return float64(i)
	}
	if n, ok := value.(float64); ok {
		return n
	}
	if n, ok := value.(json.Number); ok {
		rv, err := n.Float64()
		if err != nil {
			panic(err)
		}
		return rv
	}
	panic(fmt.Sprintf("collationToFloat64 doesn't understand %+v", value))
}
