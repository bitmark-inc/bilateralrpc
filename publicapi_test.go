// Copyright (c) 2014-2015 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bilateralrpc_test

import (
	"github.com/bitmark-inc/bilateralrpc"
	"testing"
	"time"
)

// sample server object
type Arith struct{}

// parameters
type Args struct {
	A, B int
}

// reply from one server
type Reply struct {
	S int
	D int
}

// result slice type from multicast call
type Result struct {
	From  string
	Reply Reply
	Err   error
}

// example
func (t *Arith) SumDiff(args *Args, reply *Reply) error {
	reply.S = args.A + args.B
	reply.D = args.A - args.B
	return nil
}

// ------------------------------------------------------------

// sample server object for cast
type LastItem struct {
	Value int
}

// parameters
type SetArgs struct {
	Value int
}

// null result
type NullReply struct{}

// example
func (last *LastItem) Set(args *SetArgs, reply *NullReply) error {
	last.Value = args.Value
	return nil // error will be ignored for cast
}

// ------------------------------------------------------------

// these were created by setting "show = true" below
const (
	server1PublicKey  = "J{SzFvCqHqs=zwW@eBZ5bu1F^dMydeacxYHJB*7m"
	server1PrivateKey = "cN9fB=-IypYtCcFE5rJ}(l/Ee?NhRe/?+14ibews"

	server2PublicKey  = "u[mWm{lsJG4wHGY2ae}Ljw8G2/=yH1l=%c1<kWS."
	server2PrivateKey = "glCpyPSngm{xp$+GWGHfpc!bKBRsf3C{vcCWW+U@"

	client1PublicKey  = "VQ<TKu>u*!SpZbTA=A7Dy/a<P<4kEXL9gpIG/>tS"
	client1PrivateKey = "n-FK4^]paOAS=r]=J$P?g?tycHW]V2FV7J[7[o[0"

	client2PublicKey  = "6TCclHKH3plwMujU</q=Biy?Fqq!S<5QN@U&KB1A"
	client2PrivateKey = ">LD=2&7+K$CuLBDBm.mszHhK[unH^DL=Y%K7nXm<"

	networkName = "net one"
)

func TestCreateNewKeys(t *testing.T) {
	// some code to display a generated key pair
	show := false
	// show = true // uncomment to get a new key pair
	if show {
		publicKey, privateKey, err := bilateralrpc.NewKeypair()
		if nil != err {
			t.Fatalf("NewKeypair: err = %v", err)
		}
		t.Logf("publicKey = %q  privateKey = %q", publicKey, privateKey)
	}
}

// ------------------------------------------------------------

func checkResults(t *testing.T, args Args, results []Result, serverNames []string) {

	count := len(serverNames)
	n := len(results)
	if 0 == n {
		t.Fatal("no active connections or nothing replied within the timeout")
	}
	if count != n {
		t.Errorf("want: %d results, got: %d", count, n)
	}

	t.Logf("results returned: %d", n)

	m := make(map[string]int)
	for _, serverName := range serverNames {
		m[serverName] = 0
	}

	// check result
	for i, r := range results {
		from := r.From
		if _, ok := m[from]; ok {
			m[from] += 1
		} else {
			t.Errorf("unexpected reply from: %q", from)
		}
		if args.A+args.B != r.Reply.S {
			t.Errorf("sum: %d  expected %d", r.Reply.S, args.A+args.B)
		}
		if args.A-args.B != r.Reply.D {
			t.Errorf("difference: %d  expected %d", r.Reply.D, args.A-args.B)
		}

		switch {
		case from == server1PublicKey:
			from = "Server1"
		case from == server2PublicKey:
			from = "Server2"
		case from == client1PublicKey:
			from = "Client1"
		case from == client2PublicKey:
			from = "Client2"
		}
		t.Logf("%d/%d: from: %q    S: %d  D: %d", i, n, from, r.Reply.S, r.Reply.D)
	}

	for name, count := range m {
		if 1 == count {
			continue
		}
		t.Errorf("expected 1 reply, got %d from: %q", count, name)
	}
}

// ------------------------------------------------------------

// allow time to connect - since handshake must occur in the background
func waitConnect(t *testing.T, sk *bilateralrpc.Bilateral, count int) {
	for sk.ConnectionCount() < 1 {
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("ConnectTo: %q", sk.ActiveConnections())
}

// ------------------------------------------------------------

// basic send/receive testing
func TestCall(t *testing.T) {

	// create a server
	serverName := "cleartext server name"
	server := bilateralrpc.NewPlaintext(networkName, serverName)
	defer server.Close()

	// use a specific IP:Port so client can use this - (for testing with tcp)
	serverAddress := "tcp://127.0.0.1:9979"
	//serverAddress := "inproc://test_bilateral_calling"

	// listen on a port
	if err := server.ListenOn(serverAddress); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	// register server objects
	arith := new(Arith)
	server.Register(arith)

	// create a client
	clientName := "a unique client name"
	client := bilateralrpc.NewPlaintext(networkName, clientName)
	defer client.Close()
	client.Register(arith)

	if err := client.ConnectTo(serverName, serverAddress); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	// allow time to connect - since handshake must occur in the background
	waitConnect(t, client, 1)

	// some data to send
	args := Args{
		A: 186,
		B: 253,
	}

	// this will receive slice of results
	var results []Result

	// empty string will result in calls to all active connections
	if err := client.Call([]string{}, "Arith.SumDiff", args, &results, 5*time.Second); nil != err {
		t.Fatalf("Call: err = %v", err)
	}
	checkResults(t, args, results, []string{serverName})

	// empty string will result in calls to all active connections
	if err := server.Call([]string{}, "Arith.SumDiff", args, &results, 5*time.Second); nil != err {
		t.Fatalf("reverse Call: err = %v", err)
	}
	checkResults(t, args, results, []string{clientName})
}

// basic send-only testing
func TestCast(t *testing.T) {

	// create a server
	serverName := "cleartext server name"
	server := bilateralrpc.NewPlaintext(networkName, serverName)
	defer server.Close()

	// use a specific IP:Port so client can use this - (for testing with tcp)
	serverAddress := "tcp://127.0.0.1:9979"
	//serverAddress := "inproc://test_bilateral_calling"

	// listen on a port
	if err := server.ListenOn(serverAddress); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	// register server objects
	last := new(LastItem)
	server.Register(last)

	// create a client
	clientName := "a unique client name"
	client := bilateralrpc.NewPlaintext(networkName, clientName)
	defer client.Close()
	client.Register(last)

	if err := client.ConnectTo(serverName, serverAddress); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	// allow time to connect - since handshake must occur in the background
	waitConnect(t, client, 1)

	expected := 765
	// some data to send
	args := SetArgs{
		Value: expected,
	}

	// ensure initailly zero
	if 0 != last.Value {
		t.Fatal("last.Valie is not initially zero")
	}

	// empty string will result in calls to all active connections
	if err := client.Cast([]string{}, "LastItem.Set", args); nil != err {
		t.Fatalf("Cast: err = %v", err)
	}

	// poll the server opbect for up to 5 seconds to see if cast arrived
	matches := false
	startTime := time.Now()
	for i := 0; i < 500; i += 1 {
		time.Sleep(10 * time.Millisecond)
		if last.Value == expected {
			matches = true
			break
		}
	}

	if matches {
		t.Logf("Value set after %5.3f s", time.Since(startTime).Seconds())
	} else {
		t.Fatalf("timeout on Cast: Value: %d  expected: %d", last.Value, expected)
	}
}

// one client two servers
func TestOneClientTwoServers(t *testing.T) {

	// create first server
	server1Name := "cleartext server1 name"
	server1 := bilateralrpc.NewPlaintext(networkName, server1Name)
	defer server1.Close()

	// use a specific IP:Port so client can use this - (for testing with tcp)
	server1Address := "tcp://127.0.0.1:9981"
	//server1Address := "inproc://test_bilateral_calling"

	// listen on a port
	if err := server1.ListenOn(server1Address); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	// create second server
	server2Name := "cleartext server2 name"
	server2 := bilateralrpc.NewPlaintext(networkName, server2Name)
	defer server2.Close()

	// use a specific IP:Port so client can use this - (for testing with tcp)
	server2Address := "tcp://127.0.0.1:9982"
	//server2Address := "inproc://test_bilateral_calling"

	// listen on a port
	if err := server2.ListenOn(server2Address); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	// register server objects
	arith := new(Arith)
	server1.Register(arith)
	server2.Register(arith)

	// create a client
	clientName := "a unique client name"
	client := bilateralrpc.NewPlaintext(networkName, clientName)
	defer client.Close()
	client.Register(arith)

	if err := client.ConnectTo(server1Name, server1Address); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}
	if err := client.ConnectTo(server2Name, server2Address); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	// allow time to connect - since handshake must occur in the background
	waitConnect(t, client, 2)

	// some data to send
	args := Args{
		A: 186,
		B: 253,
	}

	// this will receive slice of results
	var results []Result

	// empty string will result in calls to all active connections
	if err := client.Call([]string{}, "Arith.SumDiff", args, &results, 5*time.Second); nil != err {
		t.Fatalf("Call: err = %v", err)
	}
	checkResults(t, args, results, []string{server1Name, server2Name})

	// empty string will result in calls to all active connections
	if err := server1.Call([]string{}, "Arith.SumDiff", args, &results, 5*time.Second); nil != err {
		t.Fatalf("Call: err = %v", err)
	}
	checkResults(t, args, results, []string{clientName})

	// empty string will result in calls to all active connections
	if err := server2.Call([]string{}, "Arith.SumDiff", args, &results, 5*time.Second); nil != err {
		t.Fatalf("Call: err = %v", err)
	}
	checkResults(t, args, results, []string{clientName})
}

// ------------------------------------------------------------

// encrypted send/receive testing
func TestEncryptedCall(t *testing.T) {

	// create a server
	server1 := bilateralrpc.NewEncrypted(networkName, server1PublicKey, server1PrivateKey)
	defer server1.Close()

	// use a specific IP:Port so client can use this - (for testing with tcp)
	server1Address := "tcp://127.0.0.1:9989"
	//serverAddress := "inproc://test_bilateral_calling"

	// listen on a port
	if err := server1.ListenOn(server1Address); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	// register server objects
	arith := new(Arith)
	server1.Register(arith)

	// create a client
	client1 := bilateralrpc.NewEncrypted(networkName, client1PublicKey, client1PrivateKey)
	defer client1.Close()
	client1.Register(arith)

	if err := client1.ConnectTo(server1PublicKey, server1Address); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	// allow time to connect - since handshake must occur in the background
	waitConnect(t, client1, 1)

	// some data to send
	args := Args{
		A: 186,
		B: 253,
	}

	// this will receive slice of results
	var results []Result

	// empty string will result in calls to all active connections
	if err := client1.Call([]string{}, "Arith.SumDiff", args, &results, 5*time.Second); nil != err {
		t.Fatalf("Call: err = %v", err)
	}
	checkResults(t, args, results, []string{server1PublicKey})

	// empty string will result in calls to all active connections
	if err := server1.Call([]string{}, "Arith.SumDiff", args, &results, 5*time.Second); nil != err {
		t.Fatalf("Call: err = %v", err)
	}
	checkResults(t, args, results, []string{client1PublicKey})
}

// one client two servers
func TestEncryptedOneClientTwoServers(t *testing.T) {

	// create first server
	server1 := bilateralrpc.NewEncrypted(networkName, server1PublicKey, server1PrivateKey)
	defer server1.Close()

	// use a specific IP:Port so client can use this - (for testing with tcp)
	server1Address := "tcp://127.0.0.1:9988"
	//serverAddress := "inproc://test_bilateral_calling"

	// listen on a port
	if err := server1.ListenOn(server1Address); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	// create second server
	server2 := bilateralrpc.NewEncrypted(networkName, server2PublicKey, server2PrivateKey)
	defer server2.Close()

	// use a specific IP:Port so client can use this - (for testing with tcp)
	server2Address := "tcp://127.0.0.1:9989"
	//serverAddress := "inproc://test_bilateral_calling"

	// listen on a port
	if err := server2.ListenOn(server2Address); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	// register server objects
	arith := new(Arith)
	server1.Register(arith)
	server2.Register(arith)

	// create a client
	client1 := bilateralrpc.NewEncrypted(networkName, client1PublicKey, client1PrivateKey)
	defer client1.Close()

	if err := client1.ConnectTo(server2PublicKey, server2Address); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	if err := client1.ConnectTo(server1PublicKey, server1Address); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	client1.Register(arith)

	// allow time to connect - since handshake must occur in the background
	waitConnect(t, client1, 2)

	// some data to send
	args := Args{
		A: 186,
		B: 253,
	}

	// this will receive slice of results
	var results []Result

	// empty string will result in calls to all active connections
	if err := client1.Call([]string{}, "Arith.SumDiff", args, &results, 5*time.Second); nil != err {
		t.Fatalf("Call: err = %v", err)
	}
	checkResults(t, args, results, []string{server1PublicKey, server2PublicKey})

	// empty string will result in calls to all active connections
	if err := server1.Call([]string{}, "Arith.SumDiff", args, &results, 5*time.Second); nil != err {
		t.Fatalf("Call: err = %v", err)
	}
	checkResults(t, args, results, []string{client1PublicKey})

	// empty string will result in calls to all active connections
	if err := server2.Call([]string{}, "Arith.SumDiff", args, &results, 5*time.Second); nil != err {
		t.Fatalf("Call: err = %v", err)
	}
	checkResults(t, args, results, []string{client1PublicKey})
}

// two clients one server
func TestEncryptedTwoClientaOneServers(t *testing.T) {

	// create first server
	server1 := bilateralrpc.NewEncrypted(networkName, server1PublicKey, server1PrivateKey)
	defer server1.Close()

	// use a specific IP:Port so client can use this - (for testing with tcp)
	server1Address := "tcp://127.0.0.1:9988"
	//serverAddress := "inproc://test_bilateral_calling"

	// listen on a port
	if err := server1.ListenOn(server1Address); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	// register server objects
	arith := new(Arith)
	server1.Register(arith)

	// create first client
	client1 := bilateralrpc.NewEncrypted(networkName, client1PublicKey, client1PrivateKey)
	defer client1.Close()

	if err := client1.ConnectTo(server1PublicKey, server1Address); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	// create second client
	client2 := bilateralrpc.NewEncrypted(networkName, client2PublicKey, client2PrivateKey)
	defer client2.Close()

	if err := client2.ConnectTo(server1PublicKey, server1Address); nil != err {
		t.Fatalf("ConnectTo: err = %v", err)
	}

	client1.Register(arith)
	client2.Register(arith)

	// allow time to connect - since handshake must occur in the background
	waitConnect(t, client1, 1)
	waitConnect(t, client2, 1)

	// some data to send
	args := Args{
		A: 186,
		B: 253,
	}

	// this will receive slice of results
	var results []Result

	// empty string will result in calls to all active connections
	if err := client1.Call([]string{}, "Arith.SumDiff", args, &results, 5*time.Second); nil != err {
		t.Fatalf("Call: err = %v", err)
	}
	checkResults(t, args, results, []string{server1PublicKey})

	// empty string will result in calls to all active connections
	if err := client2.Call([]string{}, "Arith.SumDiff", args, &results, 5*time.Second); nil != err {
		t.Fatalf("Call: err = %v", err)
	}
	checkResults(t, args, results, []string{server1PublicKey})

	// empty string will result in calls to all active connections
	if err := server1.Call([]string{}, "Arith.SumDiff", args, &results, 5*time.Second); nil != err {
		t.Fatalf("Call: err = %v", err)
	}
	checkResults(t, args, results, []string{client1PublicKey, client2PublicKey})
}
