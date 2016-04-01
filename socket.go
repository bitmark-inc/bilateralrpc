// Copyright (c) 2014-2015 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bilateralrpc

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"sync"
)

type socketType int

const (
	plainSocket  = socketType(iota)
	serverSocket = socketType(iota)
	clientSocket = socketType(iota)
)

// to ensure only one auth start
var oneTimeAuthStart sync.Once

// create a socket
func createSocket(st socketType, publicKey string, privateKey string) (*zmq.Socket, error) {

	if plainSocket != st {
		oneTimeAuthStart.Do(func() {
			zmq.AuthSetVerbose(false)
			//zmq.AuthSetVerbose(true)
			err := zmq.AuthStart()
			if nil != err {
				panic(fmt.Sprintf("zmq.AuthStart(): error: %v", err))
			}
		})
	}

	socket, err := zmq.NewSocket(zmq.ROUTER)
	if nil != err {
		return socket, err
	}

	// basic socket options
	//socket.SetIpv6(true)  // ***** FIX THIS find fix for FreeBSD libzmq4 ****
	socket.SetSndtimeo(SEND_TIMEOUT)
	socket.SetLinger(LINGER_TIME)
	socket.SetRouterMandatory(0)   // discard unroutable packets
	socket.SetRouterHandover(true) // allow quick reconnect for a given public key
	socket.SetImmediate(false)     // queue messages sent to disconnected peer

	// servers identity
	socket.SetIdentity(publicKey) // just use public key for identity

	switch st {
	case serverSocket:
		// domain is servers public key
		socket.SetCurveServer(1)
		socket.SetCurvePublickey(publicKey)
		socket.SetCurveSecretkey(privateKey)
		socket.SetZapDomain(publicKey)

		// ***** FIX THIS ****
		// this allows any client to connect
		zmq.AuthCurveAdd(publicKey, zmq.CURVE_ALLOW_ANY)

		// ***** FIX THIS ****
		// maybe need to change above line to specific keys later
		//   e.g. zmq.AuthCurveAdd(serverPublicKey, client1PublicKey)
		//        zmq.AuthCurveAdd(serverPublicKey, client2PublicKey)
		// perhaps as part of ConnectTo

	case clientSocket:
		socket.SetCurvePublickey(publicKey)
		socket.SetCurveSecretkey(privateKey)

	default:
	}

	return socket, err
}
