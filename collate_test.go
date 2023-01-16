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
	"testing"

	"github.com/stretchr/testify/require"
)

type collateTest struct {
	left   interface{}
	right  interface{}
	result int
}
type collateTestList []collateTest

var collateTests, collateRawTests collateTestList

func init() {
	collateTests = collateTestList{
		// scalars
		{true, false, 1},
		{false, true, -1},
		{nil, float64(17), -1},
		{float64(1), float64(1), 0},
		{float64(123), float64(1), 1},
		{float64(123), 0123.0, 0},
		{float64(123), "123", -1},
		{"1234", "123", 1},
		{"1234", "1235", -1},
		{"1234", "1234", 0},

		// verify unicode collation
		{"a", "A", -1},
		{"A", "aa", -1},
		{"B", "aa", 1},

		// arrays
		{[]interface{}{}, "foo", 1},
		{[]interface{}{}, []interface{}{}, 0},
		{[]interface{}{true}, []interface{}{true}, 0},
		{[]interface{}{false}, []interface{}{nil}, 1},
		{[]interface{}{}, []interface{}{nil}, -1},
		{[]interface{}{float64(123)}, []interface{}{float64(45)}, 1},
		{[]interface{}{float64(123)}, []interface{}{float64(45), float64(67)}, 1},
		{[]interface{}{123.4, "wow"}, []interface{}{123.40, float64(789)}, 1},
		{[]interface{}{float64(5), "wow"}, []interface{}{float64(5), "wow"}, 0},
		{[]interface{}{float64(5), "wow"}, float64(1), 1},
		{[]interface{}{float64(1)}, []interface{}{float64(5), "wow"}, -1},

		// nested arrays
		{[]interface{}{[]interface{}{}}, []interface{}{}, 1},
		{[]interface{}{float64(1), []interface{}{float64(2), float64(3)}, float64(4)},
			[]interface{}{float64(1), []interface{}{float64(2), 3.1}, float64(4), float64(5), float64(6)}, -1},

		// unicode strings
		{"fréd", "fréd", 0},
		{"ømø", "omo", 1},
		{"\t", " ", -1},
		{"\001", " ", -1},
	}

	for _, test := range collateTests {
		jsonLeft, _ := json.Marshal(test.left)
		jsonRight, _ := json.Marshal(test.right)
		collateRawTests = append(collateRawTests, collateTest{
			left:   jsonLeft,
			right:  jsonRight,
			result: test.result,
		})
	}
}

func TestCollateJSON(t *testing.T) {
	var collator JSONCollator
	for _, test := range collateTests {
		result := collator.Collate(test.left, test.right)
		if result != test.result {
			t.Errorf("Comparing %v with %v, expected %v, got %v", test.left, test.right, test.result, result)
		}
	}
}

func TestCollateJSONRaw(t *testing.T) {
	var collator JSONCollator
	for _, test := range collateRawTests {
		result := collator.CollateRaw(test.left.([]byte), test.right.([]byte))
		if result != test.result {
			t.Errorf("CollateRawJSON `%v` with `%v`, expected %v, got %v", test.left, test.right, test.result, result)
		}
	}
}

func TestReadNumber(t *testing.T) {
	tests := []struct {
		input string
		value float64
	}{
		{"0", 0},
		{"1234", 1234},
		{"-1", -1},
		{"3.14159", 3.14159},
		{"1.7e26", 1.7e26},
		{"1.7e-10", 1.7e-10},
		{"1.7e+6", 1.7e+6},
	}
	for _, test := range tests {
		input := []byte(test.input)
		value := readNumber(&input)
		if value != test.value {
			t.Errorf("readNumber(%q) -> %f, should be %f", test.input, value, test.value)
		}
		if len(input) != 0 {
			t.Errorf("readNumber(%q), remainder is %q, should be empty", test.input, string(input))
		}

		test.input += ","
		input = []byte(test.input)
		value = readNumber(&input)
		if value != test.value {
			t.Errorf("readNumber(%q) -> %f, should be %f", test.input, value, test.value)
		}
		if string(input) != "," {
			t.Errorf("readNumber(%q), remainder is %q, should be \",\"", test.input, string(input))
		}
	}
}

func TestReadString(t *testing.T) {
	var collator JSONCollator
	tests := []struct {
		input string
		value string
	}{
		{"", ""},
		{"X", "X"},
		{"xyzzy", "xyzzy"},
		{`J.R. \"Bob\" Dobbs`, `J.R. "Bob" Dobbs`},
		{`\bFoo\t\tbar\n`, "\bFoo\t\tbar\n"},
		{`X\\y`, `X\y`},
		{`x\u0020y`, `x y`},
		{`\ufade\u0123`, "\ufade\u0123"},
	}
	for _, test := range tests {
		input := []byte("\"" + test.input + "\"")
		value := collator.readString(&input)
		if value != test.value {
			t.Errorf("readString(`%s`) -> `%s`, should be `%s`", test.input, value, test.value)
		}
	}
}

// Collate already-parsed values (this is the fastest option)
func BenchmarkCollate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var collator JSONCollator
		for _, test := range collateTests {
			result := collator.Collate(test.left, test.right)
			if result != test.result {
				panic("wrong result")
			}
		}
	}
}

// Collate raw JSON data (about 3.5x faster than parse-and-collate)
func BenchmarkCollateRaw(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var collator JSONCollator
		for _, test := range collateRawTests {
			result := collator.CollateRaw(test.left.([]byte), test.right.([]byte))
			if result != test.result {
				panic("wrong result")
			}
		}
	}
}

// Parse raw JSON and collate the values (this is the slowest)
func BenchmarkParseAndCollate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var collator JSONCollator
		for _, test := range collateRawTests {
			var left, right interface{}
			require.NoError(b, json.Unmarshal(test.left.([]byte), &left))
			require.NoError(b, json.Unmarshal(test.right.([]byte), &right))
			result := collator.Collate(left, right)
			if result != test.result {
				panic("wrong result")
			}
		}
	}
}
