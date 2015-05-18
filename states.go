// Copyright (c) 2014-2015 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bilateralrpc

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/gob"
	"errors"
	zmq "github.com/pebbe/zmq4"
	"reflect"
	"time"
)

const (
	// state machine timeouts
	FIRST_START_TIME = 2 * time.Second        // from program start to first connect request
	FIRST_JOIN_TIME  = 3 * time.Second        // time to send first join after connect
	JOIN_TIME        = 5 * time.Second        // time between join requestss
	LIVE_TIME        = 10 * time.Second       // time to send ping
	RETRY_TIME       = 10 * time.Second       // starting retry time
	SERVE_TIME       = LIVE_TIME + RETRY_TIME // timeout for server side
	BASE_PART_TIME   = 5 * time.Second        // delay before retry on a PART
	RAND_PART_TIME   = 20 * time.Millisecond  // multipled by a random byte (0..255)

	// state machine counts
	MAXIMUM_BACKOFF = 12 // backoff counter for retries
	MAXIMUM_JOINS   = 5  // number of joins before disconnect then retry

	// ØMQ settings
	LINGER_TIME  = 50 * time.Millisecond
	SEND_TIMEOUT = 0

	// default timeout for Call()
	CALL_TIMEOUT = 5 * time.Second // only used if zero is passed
)

//
type connectionState int

// state machine
const (
	stateStopped    = iota // no more activity on this connection
	stateWaiting    = iota // waiting time to connect
	stateHandshake  = iota // wait for "STAY" or "PART" response
	stateConnected  = iota // normal client running
	stateCheck      = iota // normal client running (heartbeat check)
	stateMakeServer = iota // disconnect client connection, start serving
	stateServing    = iota // normal server running
	stateDestroy    = iota // cancel loopback connection
)

type connect struct {
	name      string          // "z85 public key of corresponding to address"
	address   string          // "tcp://host:port"
	state     connectionState // stateN
	timestamp time.Time       // for time outs
	joinCount uint            // to limit JOIN requests
	backoff   uint            // exponential backoff of time out
	outgoing  bool            // client/server mode
	tick      uint64          // increments/rollover for TICK parameter
}

// incoming RPC request to server procedure
type rpcServerCallData struct {
	socket    *zmq.Socket
	from      string
	request   []byte
	wantReply bool
}

// server response to incoming RPC request
type rpcServerBackData struct {
	socket   *zmq.Socket
	to       string
	response []byte
}

// a response from a single remote node
type rpcClientResponseData struct {
	from     string // name of remote node
	response []byte // data returned
}

// a timeout response to end collection of responses
type rpcClientTimeoutData struct {
	id uint32 // RPC being timed out
}

// client RPC reply stream
type rpcClientReplyData struct {
	from  string      // node that return the reply
	reply interface{} // decoded according to resultType in rpcClientRequestData
}

// client RPC control
type rpcClientRequestData struct {
	id         uint32
	to         []string
	method     string
	args       interface{}
	structType reflect.Type
	wait       time.Duration
	done       chan *rpcClientRequestData
	reply      chan reflect.Value // rpcClientReplyData
	count      uint
}

// items in the timeout queue
type queueItem struct {
	id        uint32
	timestamp time.Time
}

// called every second
func (twoway *Bilateral) sender() (err error) {
	t := time.Now()
	for e := twoway.timeoutQueue.Front(); nil != e; e = e.Prev() {
		qi := e.Value.(queueItem)
		if qi.timestamp.After(t) {
			break
		}
		if request, ok := twoway.rpcReturns[qi.id]; ok {
			// create a timeout message
			twoway.rpcClientResponseChannel <- rpcClientTimeoutData{
				id: request.id,
			}
		}
		twoway.timeoutQueue.Remove(e)
	}

loop:
	for to, c := range twoway.connections {

		// not time yet
		if c.timestamp.After(time.Now()) {
			continue loop
		}

		switch c.state {
		case stateStopped:

		case stateMakeServer:
			log.Infof("make disconnect: %s (%s)", c.address, to)
			err = twoway.outgoingSocket.Disconnect(c.address)
			if nil != err {
				log.Infof("disconnect(MakeServer) err = %v", err)
			}
			//c.timestamp = time.Now().Add(FIRST_JOIN_TIME)
			c.timestamp = time.Now().Add(SERVE_TIME)
			c.state = stateServing

		case stateDestroy:
			log.Infof("destroy disconnect: %s (%s)", c.address, to)
			twoway.outgoingSocket.Disconnect(c.address)
			if nil != err {
				log.Infof("disconnect(Destroy) err = %v", err)
			}
			c.state = stateStopped
			delete(twoway.connections, to)

		case stateServing:
			// only get here if timed out - no client request in SERVE_TIME
			log.Info("serving timed out")
			c.state = stateStopped

		case stateWaiting:
			// if encrypted set remote public key
			if twoway.encrypted {
				twoway.outgoingSocket.SetCurveServerkey(c.name)
			}
			log.Infof("waiting connect: %s @ %s", to, c.address)
			err := twoway.outgoingSocket.Connect(c.address)
			if nil != err {
				log.Infof("connect err = %v", err)
				c.state = stateStopped
			} else {
				c.state = stateHandshake
				c.timestamp = time.Now().Add(FIRST_JOIN_TIME)
				c.joinCount = 0
			}

		case stateHandshake:
			if c.joinCount < MAXIMUM_JOINS {
				c.joinCount += 1
				c.timestamp = time.Now().Add(JOIN_TIME)
				log.Infof("join %d → %s", c.joinCount, to)
				err = sendPacket(twoway.outgoingSocket, to, "JOIN", []byte(twoway.networkName))
			} else {
				log.Infof("handshake disconnect: %s (%s)", c.address, to)
				twoway.outgoingSocket.Disconnect(c.address)
				if nil != err {
					log.Infof("disconnect(Handshake) err = %v", err)
				}
				c.state = stateWaiting
				c.timestamp = time.Now().Add(RETRY_TIME << c.backoff)
				if c.backoff < MAXIMUM_BACKOFF {
					c.backoff += 1
				}
			}

		case stateConnected:
			c.tick += 1
			log.Infof("tick %d → %s", c.tick, to)

			tickBuffer := make([]byte, 8)
			binary.BigEndian.PutUint64(tickBuffer, c.tick)
			err = sendPacket(twoway.outgoingSocket, to, "TICK", tickBuffer)

			c.timestamp = time.Now().Add(LIVE_TIME)
			c.state = stateCheck

		case stateCheck:
			log.Infof("check disconnect: %s (%s)", c.address, to)
			err := twoway.outgoingSocket.Disconnect(c.address)
			if nil != err {
				log.Infof("disconnect(Check) err = %v", err)
			}
			c.state = stateWaiting
			c.timestamp = time.Now().Add(RETRY_TIME << c.backoff)
			if c.backoff < MAXIMUM_BACKOFF {
				c.backoff += 1
			}
		}
	}

	return nil
}

// handle incoming packets on the listening socket
func (twoway *Bilateral) listenHandler() error {

	from, command, data, err := receivePacket(twoway.listenSocket)
	if nil != err {
		log.Infof("listen: from: %s: err = %v", from, err)
		return err
	}
	// prevent loopback
	if twoway.serverName == from {
		err := twoway.listenSocket.Disconnect(from)
		if nil != err {
			log.Infof("disconnect(loop) err = %v", err)
		}
		twoway.connections[from].state = stateDestroy
		return nil
	}

	log.Debugf("From←: %q  command: %q  data: %x", from, command, data)

	switch command {
	case "JOIN": // client wants to connect
		stay := false
		if c, ok := twoway.connections[from]; ok {
			log.Debugf("From←: %q  command: %q  state: %#v", from, command, c.state)
			if stateConnected == c.state || stateCheck == c.state || stateHandshake == c.state {
				// already have a connection to remote - tell client to disconnect
				stay = false
			} else {
				// remote trying to connect, so disconnect my client socket, become server
				c.state = stateMakeServer
				//c.timestamp = time.Now().Add(SERVE_TIME)
				stay = true
			}
			log.Debugf("From←: %q  command: %q  stay: %v", from, command, stay)

		} else if string(data) != twoway.networkName {
			// mismatched network name
			stay = false

		} else {
			// no connection, so put into serving mode
			twoway.connections[from] = &connect{
				timestamp: time.Now().Add(SERVE_TIME),
				address:   "?",
				state:     stateServing,
				joinCount: 0,
				backoff:   0,
				outgoing:  false,
				tick:      0,
			}
			stay = true
		}
		if stay {
			err = sendPacket(twoway.listenSocket, from, "STAY", []byte{})
		} else {
			err = sendPacket(twoway.listenSocket, from, "PART", []byte{})
		}
		if nil != err {
			log.Debugf("stay: %v  error: %v", stay, err)
		}

	case "TICK": // client checking the connection
		if c, ok := twoway.connections[from]; ok && stateServing == c.state {
			c.timestamp = time.Now().Add(SERVE_TIME)
			c.backoff = 0
			err = sendPacket(twoway.listenSocket, from, "TOCK", data)
		}

	case "CALL": // client submits a request for us to process
		if c, ok := twoway.connections[from]; ok && stateServing == c.state {
			c.timestamp = time.Now().Add(SERVE_TIME)
			c.backoff = 0
			twoway.rpcServerCallChannel <- rpcServerCallData{
				socket:    twoway.listenSocket,
				from:      from,
				request:   data,
				wantReply: true,
			}
		}

	case "CAST": // client submits a request for us to process
		if c, ok := twoway.connections[from]; ok && stateServing == c.state {
			c.timestamp = time.Now().Add(SERVE_TIME)
			c.backoff = 0
			twoway.rpcServerCallChannel <- rpcServerCallData{
				socket:    twoway.listenSocket,
				from:      from,
				request:   data,
				wantReply: false,
			}
		}

	case "BACK": // clients "server-side" replied to our request
		if c, ok := twoway.connections[from]; ok && stateServing == c.state {
			c.timestamp = time.Now().Add(SERVE_TIME)
			c.backoff = 0
			twoway.rpcClientResponseChannel <- rpcClientResponseData{
				from:     from,
				response: data,
			}
		}
	default: // ignore others
	}

	return err
}

// handle response from an upstream server
func (twoway *Bilateral) replyHandler() error {

	from, command, data, err := receivePacket(twoway.outgoingSocket)
	if nil != err {
		log.Infof("reply: from: %s: err = %v", from, err)
		return err
	}

	log.Debugf("From→: %q  command: %q  data: %x", from, command, data)

	switch command {
	case "PART": // server will disconnect this connection
		err := twoway.outgoingSocket.Disconnect(from)
		if nil != err {
			log.Infof("disconnect(PART) err = %v", err)
		}
		if c, ok := twoway.connections[from]; ok {
			log.Infof("reply:part state = %d", c.state)
			rnd := make([]byte, 1)
			rand.Read(rnd)
			c.timestamp = time.Now().Add(BASE_PART_TIME + time.Duration(rnd[0])*RAND_PART_TIME)
			c.state = stateWaiting
		}

	case "STAY": // server allows this connection
		if c, ok := twoway.connections[from]; ok && stateHandshake == c.state {
			c.state = stateConnected
			c.backoff = 0
			c.timestamp = time.Now().Add(LIVE_TIME)
		} else {
			log.Infof("ignore stay: %v", c)
		}

	case "TOCK": // keep the connection alive
		if c, ok := twoway.connections[from]; ok && stateCheck == c.state {
			c.state = stateConnected
			c.backoff = 0
			c.timestamp = time.Now().Add(LIVE_TIME)
		}

	case "CALL": // server's "client" submits a request
		if c, ok := twoway.connections[from]; ok && (stateConnected == c.state || stateCheck == c.state) {
			c.state = stateConnected
			c.backoff = 0
			c.timestamp = time.Now().Add(LIVE_TIME)
			twoway.rpcServerCallChannel <- rpcServerCallData{
				socket:    twoway.outgoingSocket,
				from:      from,
				request:   data,
				wantReply: true,
			}
		}

	case "CAST": // server's "client" submits a request
		if c, ok := twoway.connections[from]; ok && (stateConnected == c.state || stateCheck == c.state) {
			c.state = stateConnected
			c.backoff = 0
			c.timestamp = time.Now().Add(LIVE_TIME)
			twoway.rpcServerCallChannel <- rpcServerCallData{
				socket:    twoway.outgoingSocket,
				from:      from,
				request:   data,
				wantReply: false,
			}
		}

	case "BACK": // server replied to our request
		if c, ok := twoway.connections[from]; ok && (stateConnected == c.state || stateCheck == c.state) {
			c.state = stateConnected
			c.backoff = 0
			c.timestamp = time.Now().Add(LIVE_TIME)
			twoway.rpcClientResponseChannel <- rpcClientResponseData{
				from:     from,
				response: data,
			}
		}
	default: // ignore others
	}

	return err
}

// if an rpc comes in reply to server
func (twoway *Bilateral) rpcBackHandler(item interface{}) error {
	data := item.(rpcServerBackData)
	if c, ok := twoway.connections[data.to]; ok {
		switch c.state {
		case stateConnected, stateCheck, stateServing:
			sendPacket(data.socket, data.to, "BACK", data.response)
		default:
		}
	}
	return nil
}

// this servers "procedure"
func (twoway *Bilateral) rpcProcedure() {
	twoway.rpcShutdown = make(chan bool)
	defer close(twoway.rpcShutdown)

loop:
	for {
		select {
		case request := <-twoway.rpcServerCallChannel:
			// received a request for process

			go func() {
				buffer := bytes.NewBuffer(request.request)
				dec := gob.NewDecoder(buffer)

				var id uint32
				err := dec.Decode(&id)
				if nil != err {
					log.Infof("id decode err = %v", err)
				}

				var method string
				err = dec.Decode(&method)
				if nil != err {
					log.Infof("method decode err = %v", err)
				}

				log.Infof("RPC %s request: %d %s", request.from, id, method)

				responseBuffer := new(bytes.Buffer)
				enc := gob.NewEncoder(responseBuffer)

				err = enc.Encode(id)
				if err != nil {
					log.Infof("id encode err = %v", err)
				}

				err = enc.Encode(method)
				if err != nil {
					log.Infof("method encode err = %v", err)
				}

				err = twoway.server.Call(method, buffer, responseBuffer)
				if err != nil {
					log.Infof("reply error encode err = %v", err)
				}

				if request.wantReply {
					twoway.rpcServerBackChannel <- rpcServerBackData{
						socket:   request.socket,
						to:       request.from,
						response: responseBuffer.Bytes(),
					}
				}
			}()

		case <-twoway.shutdownAll:
			break loop
		}
	}
	log.Info("RPC exit")
}

// if an rpc comes in send to some/all servers
func (twoway *Bilateral) rpcClientRequestHandler(item interface{}) error {
	request, ok := item.(*rpcClientRequestData)
	if !ok {
		return nil // throw away invalid items
	}

	// determine if CALL(return value expected) or CAST(no return value possible)
	opCallOrCast := "CALL"
	if nil == request.done {
		opCallOrCast = "CAST"
	} else {
		twoway.rpcReturns[request.id] = request
	}

	buffer := bytes.Buffer{}
	enc := gob.NewEncoder(&buffer)

	err := enc.Encode(request.id)
	if err != nil {
		log.Infof("id encode err = %v", err)
	}

	err = enc.Encode(request.method)
	if err != nil {
		log.Infof("method encode err = %v", err)
	}

	err = enc.Encode(request.args)
	if err != nil {
		log.Infof("args encode err = %v", err)
	}

	packet := buffer.Bytes()

	request.count = 0
	if nil == request.to {
		for to, c := range twoway.connections {
			switch c.state {
			case stateConnected, stateCheck:
				log.Debugf("RPC/%s →%s (%x)", opCallOrCast, to, packet)
				sendPacket(twoway.outgoingSocket, to, opCallOrCast, packet)
				request.count += 1
			case stateServing:
				log.Debugf("RPC/%s %s← (%x)", opCallOrCast, to, packet)
				sendPacket(twoway.listenSocket, to, opCallOrCast, packet)
				request.count += 1
			default:
			}
		}
	} else {
		for _, to := range request.to {
			c, ok := twoway.connections[to]
			if !ok {
				continue
			}
			switch c.state {
			case stateConnected, stateCheck:
				log.Debugf("RPC/%s →%s (%x)", opCallOrCast, to, packet)
				sendPacket(twoway.outgoingSocket, to, opCallOrCast, packet)
				request.count += 1
			case stateServing:
				log.Debugf("RPC/%s %s← (%x)", opCallOrCast, to, packet)
				sendPacket(twoway.listenSocket, to, opCallOrCast, packet)
				request.count += 1
			default:
			}
		}
	}

	// nothing sent so nothing expected, just finish the request
	if 0 == request.count || nil == request.done {
		log.Infof("nothing was sent / no reply expected: count = %d", request.count)
		twoway.finishRequest(request)
		return nil
	}

	log.Debugf("make chan %d", request.count)
	request.reply = make(chan reflect.Value, request.count)

	// queue for processing
	twoway.insertEvent(request.id, request.wait)

	return nil
}

// finish the request and remove it from the map
// allowing the Call to return any accumulated results
func (twoway *Bilateral) finishRequest(request *rpcClientRequestData) {
	log.Infof("finish: %v", request)
	if nil != request.reply {
		close(request.reply)
	}
	if nil != request.done {
		request.done <- request
		close(request.done)
	}
	delete(twoway.rpcReturns, request.id)
}

// if an rpc reply comes in from a remote, match to request
func (twoway *Bilateral) rpcClientResponseHandler(item interface{}) error {

	switch item.(type) {

	case rpcClientTimeoutData: // handle a timeout message
		timeout := item.(rpcClientTimeoutData)
		if request, ok := twoway.rpcReturns[timeout.id]; ok {
			log.Infof("timeout id = %d", timeout.id)
			twoway.finishRequest(request)
		}
		return nil

	case rpcClientResponseData: // handle normal message
		response := item.(rpcClientResponseData)
		buffer := bytes.NewBuffer(response.response)
		dec := gob.NewDecoder(buffer)

		var id uint32
		err := dec.Decode(&id)
		if nil != err {
			log.Infof("id decode err = %v", err)
		}

		var method string
		err = dec.Decode(&method)
		if nil != err {
			log.Infof("method decode err = %v", err)
		}

		var errmsg string
		err = dec.Decode(&errmsg)
		if nil != err {
			log.Infof("errmsg decode err = %v", err)
		}

		// get matching request or throw away invalid/expired requests
		request, ok := twoway.rpcReturns[id]
		if !ok || request.method != method {
			log.Infof("RPC response: expired")
			return nil
		}

		// decode the result
		result := reflect.New(request.structType)
		resultElement := result.Elem()
		from := resultElement.FieldByName("From")
		replyError := resultElement.FieldByName("Err")
		reply := resultElement.FieldByName("Reply")

		log.Debugf("RPC response: result: %v", reply.Kind())

		from.SetString(response.from)

		if "" == errmsg {
			err = dec.Decode(reply.Addr().Interface())
			if nil != err {
				log.Infof("…reply decode err = %v", err)
			}
		} else {
			replyError.Set(reflect.ValueOf(errors.New(errmsg)))
		}

		log.Debugf("RPC response: %s:%d %s(...) → %#v", response.from, id, method, reply.Interface())

		// send the from/result parts of the response
		request.reply <- result

		if request.count <= 1 {
			log.Infof("RPC response: done")
			twoway.finishRequest(request)
		} else {
			log.Infof("RPC response: normal")
			request.count -= 1
		}

	default: // just throw away invalid items
	}

	return nil
}

// insert an entry into the time out queue
func (twoway *Bilateral) insertEvent(id uint32, delay time.Duration) {
	t := time.Now().Add(delay)
	v := queueItem{
		timestamp: t,
		id:        id,
	}

	for e := twoway.timeoutQueue.Front(); nil != e; e = e.Prev() {
		if t.After(e.Value.(queueItem).timestamp) {
			twoway.timeoutQueue.InsertAfter(v, e)
			return
		}
	}
	twoway.timeoutQueue.PushFront(v)
}
