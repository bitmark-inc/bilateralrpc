// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file in this directory.

// This is from go src net/rpc/server.go
//
// It is stripped down to provide only the server registration system
// so as to allow compatability with the Go library RPC callback API.

/*
	Package rpc provides access to the exported methods of an object across a
	network or other I/O connection.  A server registers an object, making it visible
	as a service with the name of the type of the object.  After registration, exported
	methods of the object will be accessible remotely.  A server may register multiple
	objects (services) of different types but it is an error to register multiple
	objects of the same type.

	Only methods that satisfy these criteria will be made available for remote access;
	other methods will be ignored:

		- the method is exported.
		- the method has two arguments, both exported (or builtin) types.
		- the method's second argument is a pointer.
		- the method has return type error.

	In effect, the method must look schematically like

		func (t *T) MethodName(argType T1, replyType *T2) error

	where T, T1 and T2 can be marshaled by encoding/gob.
	These requirements apply even if a different codec is used.
	(In the future, these requirements may soften for custom codecs.)

	The method's first argument represents the arguments provided by the caller; the
	second argument represents the result parameters to be returned to the caller.
	The method's return value, if non-nil, is passed back as a string that the client
	sees as if created by errors.New.  If an error is returned, the reply parameter
	will not be sent back to the client.

	Here is a simple example.  A server wishes to export an object of type Arith:

		package server

		type Args struct {
			A, B int
		}

		type Quotient struct {
			Quo, Rem int
		}

		type Arith int

		func (t *Arith) Multiply(args *Args, reply *int) error {
			*reply = args.A * args.B
			return nil
		}

		func (t *Arith) Divide(args *Args, quo *Quotient) error {
			if args.B == 0 {
				return errors.New("divide by zero")
			}
			quo.Quo = args.A / args.B
			quo.Rem = args.A % args.B
			return nil
		}

	The server calls:

		arith := new(Arith)
		rpc.Register(arith)
*/
package rpc

import (
	"bytes"
	//"bufio"
	"encoding/gob"
	"errors"
	//"io"
	"log"
	//"net"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

// Precompute the reflect type for error.  Can't use error directly
// because Typeof takes an empty interface value.  This is annoying.
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

// magick fields:
//
//  fields that if present in the Args paramter structure which if the
//  match the right type will be overridden by internal data values.
//  Must be prefixed by "Bilareral_" to make them clear.
const (
	mf_BlSender = "Bilateral_SENDER" // string
)

type methodType struct {
	// sync.Mutex // protects counters
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
	// numCalls   uint
}

type service struct {
	name   string                 // name of service
	rcvr   reflect.Value          // receiver of methods for the service
	typ    reflect.Type           // type of the receiver
	method map[string]*methodType // registered methods
}

// Request is a header written before every RPC call.  It is used internally
// but documented here as an aid to debugging, such as when analyzing
// network traffic.
// type Request struct {
// 	ServiceMethod string   // format: "Service.Method"
// 	Seq           uint64   // sequence number chosen by client
// 	next          *Request // for free list in Server
// }

// Response is a header written before every RPC return.  It is used internally
// but documented here as an aid to debugging, such as when analyzing
// network traffic.
// type Response struct {
// 	ServiceMethod string    // echoes that of the Request
// 	Seq           uint64    // echoes that of the request
// 	Error         string    // error, if any.
// 	next          *Response // for free list in Server
// }

// Server represents an RPC Server.
type Server struct {
	mu         sync.RWMutex // protects the serviceMap
	serviceMap map[string]*service
	// reqLock             sync.Mutex // protects freeReq
	// freeReq             *Request
	// respLock            sync.Mutex // protects freeResp
	// freeResp            *Response
}

// NewServer returns a new Server.
func NewServer() *Server {
	return &Server{serviceMap: make(map[string]*service)}
}

// DefaultServer is the default instance of *Server.
var DefaultServer = NewServer()

// Is this an exported - upper case - name?
func isExported(name string) bool {
	rune, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(rune)
}

// Is this type exported or a builtin?
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return isExported(t.Name()) || t.PkgPath() == ""
}

// Register publishes in the server the set of methods of the
// receiver value that satisfy the following conditions:
//	- exported method
//	- two arguments, both of exported type
//	- the second argument is a pointer
//	- one return value, of type error
// It returns an error if the receiver is not an exported type or has
// no suitable methods. It also logs the error using package log.
// The client accesses each method using a string of the form "Type.Method",
// where Type is the receiver's concrete type.
func (server *Server) Register(rcvr interface{}) error {
	return server.register(rcvr, "", false)
}

// RegisterName is like Register but uses the provided name for the type
// instead of the receiver's concrete type.
func (server *Server) RegisterName(name string, rcvr interface{}) error {
	return server.register(rcvr, name, true)
}

// the main regitration routine
func (server *Server) register(rcvr interface{}, name string, useName bool) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.serviceMap == nil {
		server.serviceMap = make(map[string]*service)
	}
	s := new(service)
	s.typ = reflect.TypeOf(rcvr)
	s.rcvr = reflect.ValueOf(rcvr)
	sname := reflect.Indirect(s.rcvr).Type().Name()
	if useName {
		sname = name
	}
	if sname == "" {
		s := "rpc.Register: no service name for type " + s.typ.String()
		log.Print(s)
		return errors.New(s)
	}
	if !isExported(sname) && !useName {
		s := "rpc.Register: type " + sname + " is not exported"
		log.Print(s)
		return errors.New(s)
	}
	if _, present := server.serviceMap[sname]; present {
		return errors.New("rpc: service already defined: " + sname)
	}
	s.name = sname

	// Install the methods
	s.method = suitableMethods(s.typ, true)

	if len(s.method) == 0 {
		str := ""

		// To help the user, see if a pointer receiver would work.
		method := suitableMethods(reflect.PtrTo(s.typ), false)
		if len(method) != 0 {
			str = "rpc.Register: type " + sname + " has no exported methods of suitable type (hint: pass a pointer to value of that type)"
		} else {
			str = "rpc.Register: type " + sname + " has no exported methods of suitable type"
		}
		log.Print(str)
		return errors.New(str)
	}
	server.serviceMap[s.name] = s
	return nil
}

// suitableMethods returns suitable Rpc methods of typ, it will report
// error using log if reportErr is true.
func suitableMethods(typ reflect.Type, reportErr bool) map[string]*methodType {
	methods := make(map[string]*methodType)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name
		// Method must be exported.
		if method.PkgPath != "" {
			continue
		}
		// Method needs three ins: receiver, *args, *reply.
		if mtype.NumIn() != 3 {
			if reportErr {
				log.Println("method", mname, "has wrong number of ins:", mtype.NumIn())
			}
			continue
		}
		// First arg need not be a pointer.
		argType := mtype.In(1)
		if !isExportedOrBuiltinType(argType) {
			if reportErr {
				log.Println(mname, "argument type not exported:", argType)
			}
			continue
		}
		// Second arg must be a pointer.
		replyType := mtype.In(2)
		if replyType.Kind() != reflect.Ptr {
			if reportErr {
				log.Println("method", mname, "reply type not a pointer:", replyType)
			}
			continue
		}
		// Reply type must be exported.
		if !isExportedOrBuiltinType(replyType) {
			if reportErr {
				log.Println("method", mname, "reply type not exported:", replyType)
			}
			continue
		}
		// Method needs one out.
		if mtype.NumOut() != 1 {
			if reportErr {
				log.Println("method", mname, "has wrong number of outs:", mtype.NumOut())
			}
			continue
		}
		// The return type of the method must be error.
		if returnType := mtype.Out(0); returnType != typeOfError {
			if reportErr {
				log.Println("method", mname, "returns", returnType.String(), "not error")
			}
			continue
		}
		methods[mname] = &methodType{method: method, ArgType: argType, ReplyType: replyType}
	}
	return methods
}

// A value sent as a placeholder for the server's response value when the server
// receives an invalid request. It is never decoded by the client since the Response
// contains an error when it is used.
// var invalidRequest = struct{}{}

// func (server *Server) sendResponse(sending *sync.Mutex, req *Request, reply interface{}, codec ServerCodec, errmsg string) {
// 	resp := server.getResponse()
// 	// Encode the response header
// 	resp.ServiceMethod = req.ServiceMethod
// 	if errmsg != "" {
// 		resp.Error = errmsg
// 		reply = invalidRequest
// 	}
// 	resp.Seq = req.Seq
// 	sending.Lock()
// 	err := codec.WriteResponse(resp, reply)
// 	// if debugLog && err != nil {
// 	if err != nil {
// 		log.Println("rpc: writing response:", err)
// 	}
// 	sending.Unlock()
// 	server.freeResponse(resp)
// }

// low level call to the actual method
func (s *service) call(server *Server, mtype *methodType, argv, replyv reflect.Value) string {

	function := mtype.method.Func
	// Invoke the method, providing a new value for the reply.
	returnValues := function.Call([]reflect.Value{s.rcvr, argv, replyv})
	// The return value for the method is an error.
	errInter := returnValues[0].Interface()
	errmsg := ""
	if errInter != nil {
		errmsg = errInter.(error).Error()
	}
	return errmsg
}

// execute a server side call
func (server *Server) Call(ServiceMethod string, in *bytes.Buffer, out *bytes.Buffer, sender string) (err error) {
	dot := strings.LastIndex(ServiceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc: service/method request ill-formed: " + ServiceMethod)
		return
	}
	serviceName := ServiceMethod[:dot]
	methodName := ServiceMethod[dot+1:]

	// Look up the request.
	server.mu.RLock()
	service := server.serviceMap[serviceName]
	server.mu.RUnlock()
	if service == nil {
		err = errors.New("rpc: can't find service " + ServiceMethod)
		return
	}
	mtype := service.method[methodName]
	if mtype == nil {
		err = errors.New("rpc: can't find method " + ServiceMethod)
		return
	}

	// Decode the argument value.
	argIsValue := false // if true, call requires value arg
	var argv reflect.Value
	if mtype.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(mtype.ArgType.Elem())
	} else {
		argv = reflect.New(mtype.ArgType)
		argIsValue = true // record need to dereference later
	}
	// argv guaranteed to be a pointer now.
	codec := gob.NewDecoder(in)
	if err = codec.Decode(argv.Interface()); err != nil {
		return
	}
	argvDeRef := argv.Elem() // to access the fields of the struct

	// check for magick fields and override their values
	// prior to the procedure call
	blSender := argvDeRef.FieldByName(mf_BlSender)
	if reflect.String == blSender.Kind() {
		blSender.SetString(sender)
	}

	// call needs value not pointer, so dereference
	// i.e. calling : func M(A_t args, *R_t result)
	if argIsValue {
		argv = argvDeRef
	}

	// result type
	replyv := reflect.New(mtype.ReplyType.Elem())

	errmsg := service.call(server, mtype, argv, replyv)

	outCodec := gob.NewEncoder(out)

	err = outCodec.Encode(errmsg)
	if err != nil {
		return
	}
	err = outCodec.Encode(replyv.Interface())

	return
}

// type gobServerCodec struct {
// 	rwc    io.ReadWriteCloser
// 	dec    *gob.Decoder
// 	enc    *gob.Encoder
// 	encBuf *bufio.Writer
// }

// func (c *gobServerCodec) ReadRequestHeader(r *Request) error {
// 	return c.dec.Decode(r)
// }

// func (c *gobServerCodec) ReadRequestBody(body interface{}) error {
// 	return c.dec.Decode(body)
// }

// func (c *gobServerCodec) WriteResponse(r *Response, body interface{}) (err error) {
// 	if err = c.enc.Encode(r); err != nil {
// 		return
// 	}
// 	if err = c.enc.Encode(body); err != nil {
// 		return
// 	}
// 	return c.encBuf.Flush()
// }

// func (c *gobServerCodec) Close() error {
// 	return c.rwc.Close()
// }

// // ServeConn runs the server on a single connection.
// // ServeConn blocks, serving the connection until the client hangs up.
// // The caller typically invokes ServeConn in a go statement.
// // ServeConn uses the gob wire format (see package gob) on the
// // connection.  To use an alternate codec, use ServeCodec.
// func (server *Server) ServeConn(conn io.ReadWriteCloser) {
// 	buf := bufio.NewWriter(conn)
// 	srv := &gobServerCodec{conn, gob.NewDecoder(conn), gob.NewEncoder(buf), buf}
// 	server.ServeCodec(srv)
// }

// // ServeCodec is like ServeConn but uses the specified codec to
// // decode requests and encode responses.
// func (server *Server) ServeCodec(codec ServerCodec) {
// 	sending := new(sync.Mutex)
// 	for {
// 		service, mtype, req, argv, replyv, keepReading, err := server.readRequest(codec)
// 		if err != nil {
// 			// if debugLog && err != io.EOF {
// 			// 	log.Println("rpc:", err)
// 			// }
// 			if !keepReading {
// 				break
// 			}
// 			// send a response if we actually managed to read a header.
// 			if req != nil {
// 				server.sendResponse(sending, req, invalidRequest, codec, err.Error())
// 				server.freeRequest(req)
// 			}
// 			continue
// 		}
// 		go service.call(server, sending, mtype, req, argv, replyv, codec)
// 	}
// 	codec.Close()
// }

// // ServeRequest is like ServeCodec but synchronously serves a single request.
// // It does not close the codec upon completion.
// func (server *Server) ServeRequest(codec ServerCodec) error {
// 	sending := new(sync.Mutex)
// 	service, mtype, req, argv, replyv, keepReading, err := server.readRequest(codec)
// 	if err != nil {
// 		if !keepReading {
// 			return err
// 		}
// 		// send a response if we actually managed to read a header.
// 		if req != nil {
// 			server.sendResponse(sending, req, invalidRequest, codec, err.Error())
// 			server.freeRequest(req)
// 		}
// 		return err
// 	}
// 	service.call(server, sending, mtype, req, argv, replyv, codec)
// 	return nil
// }

// func (server *Server) getRequest() *Request {
// 	server.reqLock.Lock()
// 	req := server.freeReq
// 	if req == nil {
// 		req = new(Request)
// 	} else {
// 		server.freeReq = req.next
// 		*req = Request{}
// 	}
// 	server.reqLock.Unlock()
// 	return req
// }

// func (server *Server) freeRequest(req *Request) {
// 	server.reqLock.Lock()
// 	req.next = server.freeReq
// 	server.freeReq = req
// 	server.reqLock.Unlock()
// }

// func (server *Server) getResponse() *Response {
// 	server.respLock.Lock()
// 	resp := server.freeResp
// 	if resp == nil {
// 		resp = new(Response)
// 	} else {
// 		server.freeResp = resp.next
// 		*resp = Response{}
// 	}
// 	server.respLock.Unlock()
// 	return resp
// }

// func (server *Server) freeResponse(resp *Response) {
// 	server.respLock.Lock()
// 	resp.next = server.freeResp
// 	server.freeResp = resp
// 	server.respLock.Unlock()
// }

// func (server *Server) readRequest(codec ServerCodec) (service *service, mtype *methodType, req *Request, argv, replyv reflect.Value, keepReading bool, err error) {
// 	service, mtype, req, keepReading, err = server.readRequestHeader(codec)
// 	if err != nil {
// 		if !keepReading {
// 			return
// 		}
// 		// discard body
// 		codec.ReadRequestBody(nil)
// 		return
// 	}

// 	// Decode the argument value.
// 	argIsValue := false // if true, need to indirect before calling.
// 	if mtype.ArgType.Kind() == reflect.Ptr {
// 		argv = reflect.New(mtype.ArgType.Elem())
// 	} else {
// 		argv = reflect.New(mtype.ArgType)
// 		argIsValue = true
// 	}
// 	// argv guaranteed to be a pointer now.
// 	if err = codec.ReadRequestBody(argv.Interface()); err != nil {
// 		return
// 	}
// 	if argIsValue {
// 		argv = argv.Elem()
// 	}

// 	replyv = reflect.New(mtype.ReplyType.Elem())
// 	return
// }

// func (server *Server) readRequestHeader(codec ServerCodec) (service *service, mtype *methodType, req *Request, keepReading bool, err error) {
// 	// Grab the request header.
// 	req = server.getRequest()
// 	err = codec.ReadRequestHeader(req)
// 	if err != nil {
// 		req = nil
// 		if err == io.EOF || err == io.ErrUnexpectedEOF {
// 			return
// 		}
// 		err = errors.New("rpc: server cannot decode request: " + err.Error())
// 		return
// 	}

// 	// We read the header successfully.  If we see an error now,
// 	// we can still recover and move on to the next request.
// 	keepReading = true

// 	dot := strings.LastIndex(req.ServiceMethod, ".")
// 	if dot < 0 {
// 		err = errors.New("rpc: service/method request ill-formed: " + req.ServiceMethod)
// 		return
// 	}
// 	serviceName := req.ServiceMethod[:dot]
// 	methodName := req.ServiceMethod[dot+1:]

// 	// Look up the request.
// 	server.mu.RLock()
// 	service = server.serviceMap[serviceName]
// 	server.mu.RUnlock()
// 	if service == nil {
// 		err = errors.New("rpc: can't find service " + req.ServiceMethod)
// 		return
// 	}
// 	mtype = service.method[methodName]
// 	if mtype == nil {
// 		err = errors.New("rpc: can't find method " + req.ServiceMethod)
// 	}
// 	return
// }

// // Accept accepts connections on the listener and serves requests
// // for each incoming connection.  Accept blocks; the caller typically
// // invokes it in a go statement.
// func (server *Server) Accept(lis net.Listener) {
// 	for {
// 		conn, err := lis.Accept()
// 		if err != nil {
// 			log.Fatal("rpc.Serve: accept:", err.Error()) // TODO(r): exit?
// 		}
// 		go server.ServeConn(conn)
// 	}
// }

// // Register publishes the receiver's methods in the DefaultServer.
// func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }

// // RegisterName is like Register but uses the provided name for the type
// // instead of the receiver's concrete type.
// func RegisterName(name string, rcvr interface{}) error {
// 	return DefaultServer.RegisterName(name, rcvr)
// }

// // A ServerCodec implements reading of RPC requests and writing of
// // RPC responses for the server side of an RPC session.
// // The server calls ReadRequestHeader and ReadRequestBody in pairs
// // to read requests from the connection, and it calls WriteResponse to
// // write a response back.  The server calls Close when finished with the
// // connection. ReadRequestBody may be called with a nil
// // argument to force the body of the request to be read and discarded.
// type ServerCodec interface {
// 	ReadRequestHeader(*Request) error
// 	ReadRequestBody(interface{}) error
// 	// WriteResponse must be safe for concurrent use by multiple goroutines.
// 	WriteResponse(*Response, interface{}) error

// 	Close() error
// }

// // ServeConn runs the DefaultServer on a single connection.
// // ServeConn blocks, serving the connection until the client hangs up.
// // The caller typically invokes ServeConn in a go statement.
// // ServeConn uses the gob wire format (see package gob) on the
// // connection.  To use an alternate codec, use ServeCodec.
// func ServeConn(conn io.ReadWriteCloser) {
// 	DefaultServer.ServeConn(conn)
// }

// // ServeCodec is like ServeConn but uses the specified codec to
// // decode requests and encode responses.
// func ServeCodec(codec ServerCodec) {
// 	DefaultServer.ServeCodec(codec)
// }

// // ServeRequest is like ServeCodec but synchronously serves a single request.
// // It does not close the codec upon completion.
// func ServeRequest(codec ServerCodec) error {
// 	return DefaultServer.ServeRequest(codec)
// }
