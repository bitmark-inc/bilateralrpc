// Copyright (c) 2014-2015 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bilateralrpc

import (
	"container/list"
	"errors"
	"fmt"
	svrpc "github.com/bitmark-inc/bilateralrpc/rpc"
	"github.com/bitmark-inc/logger"
	zmq "github.com/pebbe/zmq4"
	"reflect"
	"syscall"
	"time"
)

// limits
const (
	tickTime       = 50 * time.Millisecond
	reactorRunTime = 20 * time.Millisecond

	channelRequests = 0  // number of channel requests to handle each cycle (0 =>all)
	channelBuffers  = 50 // size of RPC channel buffers
)

// sent to all connections value for Call
var SendToAll = []string{}

// type to hold the client/server data
type Bilateral struct {
	// to stop and wait for shutdown
	shutdownAll     chan bool
	rpcShutdown     chan bool
	reactorShutdown chan bool

	// record all connections
	connections map[string]*connect

	// for RPC registration
	server *svrpc.Server

	// this servers identifier
	encrypted   bool
	serverName  string
	networkName string

	// map RPC IDs to their channels
	rpcReturns map[uint32]*rpcClientRequestData

	// communication
	listenSocket   *zmq.Socket
	outgoingSocket *zmq.Socket

	// RPC queues
	// for outgoing RPC
	rpcClientRequestChannel  chan interface{}
	rpcClientResponseChannel chan interface{}

	// for incoming RPC
	rpcServerCallChannel chan rpcServerCallData
	rpcServerBackChannel chan interface{}

	// timeout queue
	timeoutQueue list.List
}

// log channel for debugging
var log *logger.L

// set up the connection (can set plain or encrypted)
// Note that in encrypted mode the public key functions as the server name
// and that the netwokName must match on both sides for connection to be established
//
// for plaintext call as:  NewPlaintext(network_name, server_name)
//
// for encrypted call as:  NewEncrypted(network_name, public_key, private_key)
//      with values from:  pub, priv, err := NewKeypair()
func NewPlaintext(networkName string, serverName string) *Bilateral {
	return createBilateral(networkName, serverName)
}

func NewEncrypted(networkName string, publicKey string, privateKey string) *Bilateral {
	return createBilateral(networkName, publicKey, privateKey)
}

func createBilateral(networkName string, configuration ...string) *Bilateral {

	n := len(configuration)
	if n < 1 || n > 2 {
		panic("NewBilateral must have one or two arguments")
	}

	if nil == log {
		log = logger.New("bilateral")
	}

	twoway := new(Bilateral)

	// initialise global data
	twoway.serverName = configuration[0]
	twoway.networkName = networkName

	twoway.server = svrpc.NewServer()
	twoway.connections = make(map[string]*connect)
	twoway.rpcReturns = make(map[uint32]*rpcClientRequestData)

	// for stopping threads
	twoway.shutdownAll = make(chan bool)

	// RPC buffered streams
	twoway.rpcClientRequestChannel = make(chan interface{}, channelBuffers)
	twoway.rpcClientResponseChannel = make(chan interface{}, channelBuffers)
	twoway.rpcServerCallChannel = make(chan rpcServerCallData, channelBuffers)
	twoway.rpcServerBackChannel = make(chan interface{}, channelBuffers)

	log.Infof("server name: %q", twoway.serverName)

	var err error

	twoway.encrypted = false

	switch n {
	case 1: // plaintext connection
		twoway.encrypted = false

		// this is our server side
		twoway.listenSocket, err = createSocket(plainSocket, twoway.serverName, "")
		if nil != err {
			panic(fmt.Sprintf("listen socket: error: %v", err))
		}

		// prepare socket to connect to to multiple upstreams
		twoway.outgoingSocket, err = createSocket(plainSocket, twoway.serverName, "")
		if nil != err {
			panic(fmt.Sprintf("outgoing socket: error: %v", err))
		}

	case 2:
		twoway.encrypted = true

		// this is our server side
		twoway.listenSocket, err = createSocket(serverSocket, twoway.serverName, configuration[1])
		if nil != err {
			panic(fmt.Sprintf("listen socket: error: %v", err))
		}

		// prepare socket to connect to to multiple upstreams
		twoway.outgoingSocket, err = createSocket(clientSocket, twoway.serverName, configuration[1])
		if nil != err {
			panic(fmt.Sprintf("outgoing socket: error: %v", err))
		}
	}

	// set up the reactor to handle all sockets
	reactor := zmq.NewReactor()

	// incoming from downstream clients
	reactor.AddSocket(twoway.listenSocket, zmq.POLLIN,
		func(e zmq.State) error { return twoway.listenHandler() })

	// incoming from upstream servers
	reactor.AddSocket(twoway.outgoingSocket, zmq.POLLIN,
		func(e zmq.State) error { return twoway.replyHandler() })

	reactor.AddChannel(twoway.rpcClientRequestChannel, channelRequests,
		func(item interface{}) error { return twoway.rpcClientRequestHandler(item) })

	reactor.AddChannel(twoway.rpcClientResponseChannel, channelRequests,
		func(item interface{}) error { return twoway.rpcClientResponseHandler(item) })

	reactor.AddChannel(twoway.rpcServerBackChannel, channelRequests,
		func(item interface{}) error { return twoway.rpcBackHandler(item) })

	// start an event loop
	reactor.AddChannelTime(time.Tick(tickTime), 1,
		func(i interface{}) error { return twoway.sender() })

	// background RPC test routines
	go twoway.rpcProcedure()

	// start event loop
	go func() {
		twoway.reactorShutdown = make(chan bool)
		defer close(twoway.reactorShutdown)
		log.Info("reactor start")
		for {
			// note: re-run reactor if its internal poll fails with "interrupted system call"
			err := reactor.Run(reactorRunTime)
			if nil == err {
				break // normal termination
			} else if zmq.Errno(syscall.EINTR) != zmq.AsErrno(err) {
				//log.Errorf("reactor failed: error: %v", err)
				break
			}
			log.Errorf("reactor retry: error: %v", err)
		}
		log.Info("reactor stopped")
	}()

	return twoway
}

// shutdown all connections, terminate all background processing
func (twoway *Bilateral) Close() {
	log.Info("closing…")

	// disconnect all outgoing connections
	for to := range twoway.connections {
		twoway.DisconnectFrom(to)
	}

	// shutdown background processing
	close(twoway.shutdownAll)

	// close sockets so reactor will stop
	twoway.listenSocket.Close()
	twoway.outgoingSocket.Close()

	log.Info("Close: waiting…")

	// wait for final shutdown
	<-twoway.rpcShutdown
	<-twoway.reactorShutdown

	// close any remaining channels
	close(twoway.rpcClientRequestChannel)
	close(twoway.rpcClientResponseChannel)
	close(twoway.rpcServerCallChannel)
	close(twoway.rpcServerBackChannel)

	// terminate all outstanding calls
	for _, request := range twoway.rpcReturns {
		twoway.finishRequest(request)
	}

	// clear the timeout queue
	for e := twoway.timeoutQueue.Front(); nil != e; e = e.Prev() {
		twoway.timeoutQueue.Remove(e)
	}
	log.Info("Close: complete")
}

// listen for incomming requests
func (twoway *Bilateral) ListenOn(address string) error {
	log.Infof("set listen on: %s", address)
	return twoway.listenSocket.Bind(address)
}

// connect to a remote
func (twoway *Bilateral) ConnectTo(name string, address string) error {
	// do not make a loopback connection
	if twoway.serverName == name {
		return errors.New("loopback forbidden")
	}

	if _, exists := twoway.connections[name]; exists {
		return errors.New("duplicate connection")
	}

	log.Infof("connect to: %s @ %s", name, address)

	twoway.connections[name] = &connect{
		timestamp: time.Now().Add(FIRST_START_TIME),
		name:      name,
		address:   address,
		state:     stateWaiting,
		joinCount: 0,
		backoff:   0,
		outgoing:  true,
		tick:      0,
	}

	return nil
}

// disconnect from a remote
func (twoway *Bilateral) DisconnectFrom(name string) error {
	_, exists := twoway.connections[name]
	if !exists {
		return errors.New("no existing connection")
	}
	log.Infof("disconnect from: %s", name)
	delete(twoway.connections, name)
	return nil
}

// count active connections
func (twoway *Bilateral) ConnectionCount() int {
	count := 0
	for _, c := range twoway.connections {
		if stateConnected == c.state || stateCheck == c.state || stateServing == c.state {
			count += 1
		}
	}
	return count
}

// register a callback object
func (twoway *Bilateral) Register(receiver interface{}) error {
	return twoway.server.Register(receiver)
}

// get a list of acive peers names
func (twoway *Bilateral) ActiveConnections() []string {
	active := make([]string, 0, len(twoway.connections))
	for name, c := range twoway.connections {
		if stateConnected == c.state || stateCheck == c.state || stateServing == c.state {
			active = append(active, name)
		}
	}
	return active
}

// rpc call routine and receive reply, waiting if necessary
// to can be set to SendToAll to do a broadcast
func (twoway *Bilateral) Call(to []string, method string, args interface{}, results interface{}, optionalTimeout ...time.Duration) error {

	if nil != to && 0 == len(to) {
		to = nil
	}

	timeout := DEFAULT_CALL_TIMEOUT
	if len(optionalTimeout) == 1 {
		timeout = optionalTimeout[0]
	} else if len(optionalTimeout) == 1 {
		return errors.New("too many timeout values")
	}
	if timeout < MINIMUM_CALL_TIMEOUT {
		return errors.New("timeout is too short")
	}

	select {
	case <-twoway.shutdownAll:
		return errors.New("RPC system shutting down")
	default:
	}

	ptr := reflect.TypeOf(results)
	if reflect.Ptr != ptr.Kind() && !reflect.ValueOf(results).IsNil() && reflect.Slice != ptr.Elem().Kind() {
		return errors.New("must be a non-nil pointer to slice")
	}

	theSlice := ptr.Elem()

	// ptr to slice of 's' - some kind of struct
	theStruct := theSlice.Elem()
	if reflect.Struct != theStruct.Kind() || 3 != theStruct.NumField() {
		return errors.New("must be a struct with three fields")
	}

	fromField, hasFrom := theStruct.FieldByName("From")
	_, hasReply := theStruct.FieldByName("Reply")
	errField, hasErr := theStruct.FieldByName("Err")

	if !hasFrom || !hasReply || !hasErr {
		return errors.New("must be a struct with From, Reply and Err fields")
	}

	if reflect.String != fromField.Type.Kind() {
		return errors.New("struct From field must be string")
	}

	if reflect.Interface != errField.Type.Kind() {
		return errors.New("struct Err field must be error")
	}

	done := make(chan *rpcClientRequestData)

	log.Infof("send for method: %s", method)
	twoway.rpcClientRequestChannel <- &rpcClientRequestData{
		id:         nonce(),
		to:         to,
		timeout:    timeout,
		method:     method,
		args:       args,
		structType: theStruct,
		done:       done,
		reply:      nil, // added later
		count:      0,   // added later
	}

	// do retries
	var request *rpcClientRequestData
loop:
	for retry := 0; retry < DEFAULT_RETRIES; retry += 1 {
		log.Infof("wait for result for method: %s", method)

		// the processed request will be returned here
		request = <-done
		log.Debugf("request for method in loop: %s : %v", method, request)

		if nil != request.reply && len(request.reply) > 0 {
			break loop
		}

		done = make(chan *rpcClientRequestData)
		twoway.rpcClientRequestChannel <- &rpcClientRequestData{
			id:         nonce(),
			to:         to,
			timeout:    timeout,
			method:     method,
			args:       args,
			structType: theStruct,
			done:       done,
			reply:      nil, // added later
			count:      0,   // added later
		}
		log.Infof("timeout method: %s : %v", method, request)
	}

	log.Debugf("request for method: %s : %v", method, request)

	if nil == request.reply || len(request.reply) == 0 {
		return errors.New("no results")
	}

	received := reflect.MakeSlice(theSlice, 0, len(to))
	for r := range request.reply {
		received = reflect.Append(received, r.Elem())
	}
	reflect.ValueOf(results).Elem().Set(received)
	log.Debugf("Result for method: %s : %v", method, results)

	return nil
}

// rpc cast routine - no reply will be sent by remote
// to can be set to SendToAll to do a broadcast
func (twoway *Bilateral) Cast(to []string, method string, args interface{}) error {

	if nil != to && 0 == len(to) {
		to = nil
	}

	select {
	case <-twoway.shutdownAll:
		return errors.New("RPC system shutting down")
	default:
	}

	id := nonce()
	twoway.rpcClientRequestChannel <- &rpcClientRequestData{
		id:         id,
		to:         to,
		timeout:    DEFAULT_CALL_TIMEOUT, // not used
		method:     method,
		args:       args,
		structType: nil,
		done:       nil,
		reply:      nil, // added later
		count:      0,   // added later
	}

	return nil
}
