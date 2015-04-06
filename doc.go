// Copyright (c) 2014-2015 Bitmark Inc.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

/*

   Package bilateralrpc implements RPC calls among connected system
   in either direction across a single ØMQ connection.

   i.e. The connection is symmetrical and either end can call
        procedures registered in the other

   The call is by default multicast with a timeout and collects as
   many of the destination results available within a timeout.

   // the procedures
   // --------------

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
   }

   // example
   func (t *Arith) SumDiff(args *Args, reply *Reply) error {
	reply.S = args.A + args.B
	reply.D = args.A - args.B
	return nil
   }

   // the client/server setup code
   // ----------------------------

   // start up the communication system
   server := bilateralrpc.NewBilateral("myserver")
   defer server.Close()

   // listen on ports (multiple ListenOn allowed)
   server.ListenOn("tcp://*:5555")

   // register server objects
   arith := new(Arith)
   server.Register(arith)

   // many connections can be made like this:
   to := "yourserver"
   err := server.ConnectTo(to, "tcp://127.0.0.1:5566")
   if nil != err {
	panic(fmt.Sprintf("connect to %s err = %v\n", to, err))
   }


   // calling an RPC
   // --------------

   args := Args{
       A: 100,
       B: 250,
   }

   // this will receive slice of results
   var results []Result

   // empty string will result in calls to all active connections
   err = server.Call([]string{}, "Arith.SumDiff", args, &results, 5*time.Second)

   if nil != err {
       panic(fmt.Sprintf("err = %v", err))
   }

   if 0 == len(results) {
       // there were no active connections or nothing replied within the timeout
   }

*/
package bilateralrpc
