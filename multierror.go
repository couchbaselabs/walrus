// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package walrus

import "fmt"

type multiError []error

func (m multiError) Error() string {
	if len(m) == 0 {
		panic("Error of none")
	}

	return fmt.Sprintf("{%v errors, starting with %v}", len(m), m[0].Error())
}
