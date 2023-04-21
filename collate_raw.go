// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package walrus

import (
	"fmt"
	"strconv"
	"unicode/utf8"
)

type token int8

// JSON input tokens. The order is significant: it's the collation ordering defined by CouchDB.
const (
	kEndArray = token(iota)
	kEndObject
	kComma
	kColon
	kNull
	kFalse
	kTrue
	kNumber
	kString
	kArray
	kObject
)

// Collates raw JSON data without unmarshaling it.
// THE INPUTS MUST BE VALID JSON, WITH NO WHITESPACE!
// Invalid input will result in a panic, or perhaps just bogus output.
func (c *JSONCollator) CollateRaw(key1, key2 []byte) int {
	depth := 0
	for {
		c1 := key1[0]
		c2 := key2[0]
		tok1 := tokenize(c1)
		tok2 := tokenize(c2)

		// If token types don't match, stop and return their relative ordering:
		if tok1 != tok2 {
			return compareTokens(tok1, tok2)
		} else {
			switch tok1 {
			case kNull, kTrue:
				advance(&key1, 4)
				advance(&key2, 4)
			case kFalse:
				advance(&key1, 5)
				advance(&key2, 5)
			case kNumber:
				if diff := compareFloats(readNumber(&key1), readNumber(&key2)); diff != 0 {
					return diff
				}
			case kString:
				if diff := c.compareStrings(c.readString(&key1), c.readString(&key2)); diff != 0 {
					return diff
				}
			case kArray, kObject:
				advance(&key1, 1)
				advance(&key2, 1)
				depth++
			case kEndArray, kEndObject:
				advance(&key1, 1)
				advance(&key2, 1)
				depth--
			case kComma, kColon:
				advance(&key1, 1)
				advance(&key2, 1)
			}
		}
		if depth == 0 {
			return 0
		}
	}
}

func tokenize(c byte) token {
	switch c {
	case 'n':
		return kNull
	case 'f':
		return kFalse
	case 't':
		return kTrue
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-':
		return kNumber
	case '"':
		return kString
	case ']':
		return kEndArray
	case '}':
		return kEndObject
	case ',':
		return kComma
	case ':':
		return kColon
	case '[':
		return kArray
	case '{':
		return kObject
	default:
		panic(fmt.Sprintf("Unexpected character '%c' parsing JSON", c))
	}
}

// Removes n bytes from the start of the slice
func advance(s *[]byte, n int) {
	*s = (*s)[n:]
}

// Simple byte comparison
func compareTokens(a, b token) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// Parse a JSON number from the input stream
func readNumber(input *[]byte) float64 {
	// Look for the end of the number, either at a delimiter or at end of input:
	end := len(*input)
	for i, c := range *input {
		if c == ',' || c == ']' || c == '}' {
			end = i
			break
		}
	}
	numPart := string((*input)[0:end])
	result, _ := strconv.ParseFloat(numPart, 64)
	*input = (*input)[end:]
	return result
}

// Parse a JSON string from the input stream (starting at the opening quote)
func (c *JSONCollator) readString(input *[]byte) string {
	// Look for the quote marking the end of the string. Count up escape sequence:
	i := 1
	escapes := 0
	for {
		c := (*input)[i]
		if c == '"' {
			break
		} else if c == '\\' {
			escapes++
			i++
			if (*input)[i] == 'u' {
				i += 4 // skip past Unicode escape /uxxxx
			}
		}
		i++
	}

	var str string
	if escapes > 0 {
		str = c.readEscapedString((*input)[1:i], i-escapes) // slower case
	} else {
		str = string((*input)[1:i])
	}
	*input = (*input)[i+1:] // Skip the closing quote as well
	return str
}

// Parse a string, interpreting JSON escape sequences:
func (c *JSONCollator) readEscapedString(input []byte, bufSize int) string {
	decoded := make([]byte, 0, bufSize)
	for i := 0; i < len(input); i++ {
		c := input[i]
		if c == '\\' {
			i++
			c = input[i]
			if c == 'u' {
				// Decode a Unicode escape:
				r, _ := strconv.ParseUint(string(input[i+1:i+5]), 16, 32)
				i += 4
				var utf [8]byte
				size := utf8.EncodeRune(utf[0:], rune(r))
				decoded = append(decoded, utf[0:size]...)
			} else {
				switch c {
				case 'b':
					c = '\b'
				case 'n':
					c = '\n'
				case 'r':
					c = '\r'
				case 't':
					c = '\t'
				}
				decoded = append(decoded, c)
			}
		} else {
			decoded = append(decoded, c)
		}
	}
	return string(decoded)
	// This can be optimized by scanning through input for the next backslash,
	// then appending all the chars up to it in one append() call.
}
