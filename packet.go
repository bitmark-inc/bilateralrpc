// Copyright (c) 2014-2015 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bilateralrpc

import (
	zmq "github.com/pebbe/zmq4"
)

// send a packet
func sendPacket(socket *zmq.Socket, to string, command string, data []byte) error {

	m1, err := socket.Send(to, zmq.SNDMORE|zmq.DONTWAIT)
	if nil != err {
		return err
	}
	m2, err := socket.Send(command, zmq.SNDMORE|zmq.DONTWAIT)
	if nil != err {
		return err
	}
	m3, err := socket.SendBytes(data, 0|zmq.DONTWAIT)

	n1 := len(to)
	n2 := len(command)
	n3 := len(data)

	log.Infof("sp: %d/%d  %d/%d  %d/%d", m1, n1, m2, n2, m3, n3)

	return err
}

// receive a packet
func receivePacket(socket *zmq.Socket) (from string, command string, data []byte, err error) {

	from, err = socket.Recv(0)
	if nil != err {
		return "", "", []byte{}, err
	}

	if more, _ := socket.GetRcvmore(); !more {
		return from, "", []byte{}, nil
	}

	command, err = socket.Recv(0)
	if nil != err {
		return "", "", []byte{}, err
	}

	if more, _ := socket.GetRcvmore(); !more {
		return from, command, []byte{}, nil
	}

	data, err = socket.RecvBytes(0)
	if nil != err {
		return "", "", []byte{}, err
	}
	return from, command, data, nil
}
