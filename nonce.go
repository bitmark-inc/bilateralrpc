// Copyright (c) 2014-2015 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bilateralrpc

import (
	"sync/atomic"
)

// client RPC ID allocation
var globalID = uint32(0)

// get a unique id for an RPC call
func nonce() uint32 {
	return atomic.AddUint32(&globalID, 1)
}
