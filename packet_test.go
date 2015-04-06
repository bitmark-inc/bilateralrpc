// Copyright (c) 2014-2015 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bilateralrpc

import (
	zmq "github.com/pebbe/zmq4"
	"testing"
	"time"
)

const (
	server1PublicKey  = "J{SzFvCqHqs=zwW@eBZ5bu1F^dMydeacxYHJB*7m"
	server1PrivateKey = "cN9fB=-IypYtCcFE5rJ}(l/Ee?NhRe/?+14ibews"

	server2PublicKey  = "KF><C/y<l(7.>4*JyGf+N-g@d{@tg[vW4?CATndb"
	server2PrivateKey = "<qjOF1hWQofPk3q7AR#P:7bow(AQ]wY9B9+}GcHZ"

	client1PublicKey  = "VQ<TKu>u*!SpZbTA=A7Dy/a<P<4kEXL9gpIG/>tS"
	client1PrivateKey = "n-FK4^]paOAS=r]=J$P?g?tycHW]V2FV7J[7[o[0"

	client2PublicKey  = "}{jFu(p76ahKL3ZhQE(SJg1jXNNqI5%gj!naNEC-"
	client2PrivateKey = "?UpxHB:mELMu0PRL}=dMcd2Cv9THmPUUO^ts?L46"
)

// basic send/receive testing
func TestPacket(t *testing.T) {

	// use a specific IP:Port so client can use this - (for testing with tcp)
	server1Address := "tcp://127.0.0.1:9977"
	server2Address := "tcp://127.0.0.1:9978"
	delay := 1 * time.Second
	//server1Address := "inproc://test_bilateral_packet_1"
	//server2Address := "inproc://test_bilateral_packet_2"
	//delay = 0

	// plaintext names
	server1Name := "this is the #1 server"
	server2Name := "here is server two"
	client1Name := "client number one"
	client2Name := "the second client"

	// Server 1
	// --------

	listenSocket1, err := createSocket(plainSocket, server1Name, "")
	if nil != err {
		t.Fatalf("listen socket 2: err = %v", err)
	}
	defer listenSocket1.Close()

	listenSocket1.Bind(server1Address)

	// Server 2
	// --------

	listenSocket2, err := createSocket(plainSocket, server2Name, "")
	if nil != err {
		t.Fatalf("listen socket 2: err = %v", err)
	}
	defer listenSocket2.Close()

	listenSocket2.Bind(server2Address)

	// Client 1
	// --------

	outgoingSocket1, err := createSocket(plainSocket, client1Name, "")
	if nil != err {
		t.Fatalf("outgoing socket 1: err = %v", err)
	}
	defer outgoingSocket1.Close()

	outgoingSocket1.Connect(server1Address)
	outgoingSocket1.Connect(server2Address)

	// Client 2
	// --------

	outgoingSocket2, err := createSocket(plainSocket, client2Name, "")
	if nil != err {
		t.Fatalf("outgoing socket 2: err = %v", err)
	}
	defer outgoingSocket2.Close()

	outgoingSocket2.Connect(server1Address)
	outgoingSocket2.Connect(server2Address)

	// tests
	// -----

	clients := []socketKey{
		{outgoingSocket1, client1Name},
		{outgoingSocket2, client2Name},
	}

	servers := []socketKey{
		{listenSocket1, server1Name},
		{listenSocket2, server2Name},
	}

	sendReceive(t, clients, servers, delay)
}

type socketKey struct {
	socket *zmq.Socket
	name   string
}

func sendReceive(t *testing.T, clients []socketKey, servers []socketKey, delay time.Duration) {

	testInfo := []struct {
		command string
		data    string
	}{
		{"ONE", "data record one"},
		{"TWO", "data record two"},
		{"words", "The quick brown fox jumps over the lazy dog."},
		{"1234567890", "abcdefghijklmnopqrstuvwxyz01234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890"},
	}

	// delay to allow socket negotiation; or sends will be dropped
	if 0 != delay {
		time.Sleep(delay)
	}

	const separator = " →→→@SERVER: "

	// run all tests
	for i, info := range testInfo {
		for _, server := range servers {
			for _, client := range clients {
				err := sendPacket(client.socket, server.name, info.command, []byte(info.data+separator+server.name))
				if nil != err {
					t.Fatalf("sendPacket: err = %v", err)
				}
			}
		}

		for si, server := range servers {
			for ci, client := range clients {
				from, command, data, err := receivePacket(server.socket)
				if nil != err {
					t.Fatalf("receivePacket: err = %v", err)
				}
				if client.name != from {
					t.Errorf("%d: from: %q  expected: %q", i, from, client)
				}
				if info.command != command {
					t.Errorf("%d: command: %q  expected: %q", i, command, info.command)
				}
				expectedData := info.data + separator + server.name
				if expectedData != string(data) {
					t.Errorf("%d: data: %q  expected: %q", i, data, expectedData)
				}

				t.Logf("T%d/S%d/C%d: from: %q  command: %q  data: %q", i, si, ci, from, command, data)
			}
		}
	}
}

// basic send/receive testing
func TestCryptoPacket(t *testing.T) {

	// use a specific IP:Port so client can use this - (for testing with tcp)
	server1Address := "tcp://127.0.0.1:9987"
	server2Address := "tcp://127.0.0.1:9988"
	delay := 1 * time.Second
	//server1Address := "inproc://test_bilateral_packet_1"
	//server2Address := "inproc://test_bilateral_packet_2"
	//delay = 0

	// Server 1
	// --------

	listenSocket1, err := createSocket(serverSocket, server1PublicKey, server1PrivateKey)
	if nil != err {
		t.Fatalf("listen socket: err = %v", err)
	}
	defer listenSocket1.Close()

	listenSocket1.Bind(server1Address)

	// Server 2
	// --------

	listenSocket2, err := createSocket(serverSocket, server2PublicKey, server2PrivateKey)
	if nil != err {
		t.Fatalf("listen socket: err = %v", err)
	}
	defer listenSocket2.Close()

	listenSocket2.Bind(server2Address)

	// Client 1
	// --------

	outgoingSocket1, err := createSocket(clientSocket, client1PublicKey, client1PrivateKey)
	if nil != err {
		t.Fatalf("outgoing socket 1: err = %v", err)
	}
	defer outgoingSocket1.Close()

	outgoingSocket1.SetCurveServerkey(server1PublicKey)
	outgoingSocket1.Connect(server1Address)

	outgoingSocket1.SetCurveServerkey(server2PublicKey)
	outgoingSocket1.Connect(server2Address)

	// Client 2
	// --------

	outgoingSocket2, err := createSocket(clientSocket, client2PublicKey, "")
	if nil != err {
		t.Fatalf("outgoing socket 2: err = %v", err)
	}
	defer outgoingSocket2.Close()

	outgoingSocket2.ClientAuthCurve(server1PublicKey, client2PublicKey, client2PrivateKey)
	outgoingSocket2.Connect(server1Address)

	outgoingSocket2.ClientAuthCurve(server2PublicKey, client2PublicKey, client2PrivateKey)
	outgoingSocket2.Connect(server2Address)

	// tests
	// -----

	clients := []socketKey{
		{outgoingSocket1, client1PublicKey},
		{outgoingSocket2, client2PublicKey},
	}

	servers := []socketKey{
		{listenSocket1, server1PublicKey},
		{listenSocket2, server2PublicKey},
	}

	sendReceive(t, clients, servers, delay)
}
