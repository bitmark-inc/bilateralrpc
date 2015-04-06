// Copyright (c) 2014-2015 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bilateralrpc

import (
	zmq "github.com/pebbe/zmq4"
)

// create a new key pair
func NewKeypair() (publicKey string, privateKey string, err error) {
	publicKey, privateKey, err = zmq.NewCurveKeypair()
	return
}
