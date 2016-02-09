// Copyright 2015 Netflix, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/netflix/rend/binprot"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/handlers/memcached"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/textprot"
)

// Set up more sane GOGC default
func init() {
	if _, set := os.LookupEnv("GOGC"); !set {
		debug.SetGCPercent(200)
	}
}

// Setting up signal handlers
func init() {
	sigs := make(chan os.Signal)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		<-sigs
		panic("Keyboard Interrupt")
	}()
}

// Set up http debug and metrics endpoint
func init() {
	go http.ListenAndServe("localhost:11299", nil)
}

// Set up the counters used in this file
var (
	MetricConnectionsEstablishedExt = metrics.AddCounter("conn_established_ext")
	MetricConnectionsEstablishedL1  = metrics.AddCounter("conn_established_l1")
	MetricConnectionsEstablishedL2  = metrics.AddCounter("conn_established_l2")
	MetricCmdGet                    = metrics.AddCounter("cmd_get")
	MetricCmdGetL1                  = metrics.AddCounter("cmd_get_l1")
	MetricCmdGetL2                  = metrics.AddCounter("cmd_get_l2")
	MetricCmdGetHits                = metrics.AddCounter("cmd_get_hits")
	MetricCmdGetHitsL1              = metrics.AddCounter("cmd_get_hits_l1")
	MetricCmdGetHitsL2              = metrics.AddCounter("cmd_get_hits_l2")
	MetricCmdGetMisses              = metrics.AddCounter("cmd_get_misses")
	MetricCmdGetMissesL1            = metrics.AddCounter("cmd_get_misses_l1")
	MetricCmdGetMissesL2            = metrics.AddCounter("cmd_get_misses_l2")
	MetricCmdGetErrors              = metrics.AddCounter("cmd_get_errors")
	MetricCmdGetErrorsL1            = metrics.AddCounter("cmd_get_errors_l1")
	MetricCmdGetErrorsL2            = metrics.AddCounter("cmd_get_errors_l2")
	MetricCmdGetKeys                = metrics.AddCounter("cmd_get_keys")
	MetricCmdGetKeysL1              = metrics.AddCounter("cmd_get_keys_l1")
	MetricCmdGetKeysL2              = metrics.AddCounter("cmd_get_keys_l2")
	MetricCmdSet                    = metrics.AddCounter("cmd_set")
	MetricCmdSetL1                  = metrics.AddCounter("cmd_set_l1")
	MetricCmdSetL2                  = metrics.AddCounter("cmd_set_l2")
	MetricCmdSetSuccess             = metrics.AddCounter("cmd_set_success")
	MetricCmdSetSuccessL1           = metrics.AddCounter("cmd_set_success_l1")
	MetricCmdSetSuccessL2           = metrics.AddCounter("cmd_set_success_l2")
	MetricCmdSetErrors              = metrics.AddCounter("cmd_set_errors")
	MetricCmdSetErrorsL1            = metrics.AddCounter("cmd_set_errors_l1")
	MetricCmdSetErrorsL2            = metrics.AddCounter("cmd_set_errors_l2")
	MetricCmdDelete                 = metrics.AddCounter("cmd_delete")
	MetricCmdDeleteL1               = metrics.AddCounter("cmd_delete_l1")
	MetricCmdDeleteL2               = metrics.AddCounter("cmd_delete_l2")
	MetricCmdDeleteHits             = metrics.AddCounter("cmd_delete_hits")
	MetricCmdDeleteHitsL1           = metrics.AddCounter("cmd_delete_hits_l1")
	MetricCmdDeleteHitsL2           = metrics.AddCounter("cmd_delete_hits_l2")
	MetricCmdDeleteMisses           = metrics.AddCounter("cmd_delete_misses")
	MetricCmdDeleteMissesL1         = metrics.AddCounter("cmd_delete_misses_l1")
	MetricCmdDeleteMissesL2         = metrics.AddCounter("cmd_delete_misses_l2")
	MetricCmdDeleteErrors           = metrics.AddCounter("cmd_delete_errors")
	MetricCmdDeleteErrorsL1         = metrics.AddCounter("cmd_delete_errors_l1")
	MetricCmdDeleteErrorsL2         = metrics.AddCounter("cmd_delete_errors_l2")
	MetricCmdTouch                  = metrics.AddCounter("cmd_touch")
	MetricCmdTouchL1                = metrics.AddCounter("cmd_touch_l1")
	MetricCmdTouchL2                = metrics.AddCounter("cmd_touch_l2")
	MetricCmdTouchHits              = metrics.AddCounter("cmd_touch_hits")
	MetricCmdTouchHitsL1            = metrics.AddCounter("cmd_touch_hits_l1")
	MetricCmdTouchHitsL2            = metrics.AddCounter("cmd_touch_hits_l2")
	MetricCmdTouchMisses            = metrics.AddCounter("cmd_touch_misses")
	MetricCmdTouchMissesL1          = metrics.AddCounter("cmd_touch_misses_l1")
	MetricCmdTouchMissesL2          = metrics.AddCounter("cmd_touch_misses_l2")
	MetricCmdTouchErrors            = metrics.AddCounter("cmd_touch_errors")
	MetricCmdTouchErrorsL1          = metrics.AddCounter("cmd_touch_errors_l1")
	MetricCmdTouchErrorsL2          = metrics.AddCounter("cmd_touch_errors_l2")
	MetricCmdGat                    = metrics.AddCounter("cmd_gat")
	MetricCmdGatL1                  = metrics.AddCounter("cmd_gat_l1")
	MetricCmdGatL2                  = metrics.AddCounter("cmd_gat_l2")
	MetricCmdGatHits                = metrics.AddCounter("cmd_gat_hits")
	MetricCmdGatHitsL1              = metrics.AddCounter("cmd_gat_hits_l1")
	MetricCmdGatHitsL2              = metrics.AddCounter("cmd_gat_hits_l2")
	MetricCmdGatMisses              = metrics.AddCounter("cmd_gat_misses")
	MetricCmdGatMissesL1            = metrics.AddCounter("cmd_gat_misses_l1")
	MetricCmdGatMissesL2            = metrics.AddCounter("cmd_gat_misses_l2")
	MetricCmdGatErrors              = metrics.AddCounter("cmd_gat_errors")
	MetricCmdGatErrorsL1            = metrics.AddCounter("cmd_gat_errors_l1")
	MetricCmdGatErrorsL2            = metrics.AddCounter("cmd_gat_errors_l2")
	MetricCmdUnknown                = metrics.AddCounter("cmd_unknown")
	MetricErrAppError               = metrics.AddCounter("err_app_err")
	MetricErrUnrecoverable          = metrics.AddCounter("err_unrecoverable")

	HistGet   = metrics.AddHistogram("get")
	HistGetL1 = metrics.AddHistogram("get_l1")
	HistGetL2 = metrics.AddHistogram("get_l2")

	// TODO: inconsistency metrics for when L1 is not a subset of L2
)

// And away we go
func main() {
	server, err := net.Listen("tcp", ":11211")

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	for {
		remote, err := server.Accept()

		if err != nil {
			fmt.Println(err.Error())
			remote.Close()
			continue
		}

		metrics.IncCounter(MetricConnectionsEstablishedExt)

		l1conn, err := net.Dial("unix", "/tmp/memcached.sock")

		if err != nil {
			fmt.Println(err.Error())
			if l1conn != nil {
				l1conn.Close()
			}
			remote.Close()
			continue
		}

		metrics.IncCounter(MetricConnectionsEstablishedL1)

		l1 := memcached.NewChunkedHandler(l1conn)
		//l1 := memcached.NewHandler(l1conn)

		go handleConnection(remote, l1, nil)
	}
}

func abort(toClose []io.Closer, err error, binary bool) {
	if err != io.EOF {
		fmt.Println("Error while processing request. Closing connection. Error:", err.Error())
	}
	// use proper serializer to respond here
	for _, c := range toClose {
		if c != nil {
			c.Close()
		}
	}
	//panic(err)
}

func identifyPanic() string {
	var name, file string
	var line int
	var pc [16]uintptr

	n := runtime.Callers(3, pc[:])
	for _, pc := range pc[:n] {
		fn := runtime.FuncForPC(pc)
		if fn == nil {
			continue
		}
		file, line = fn.FileLine(pc)
		name = fn.Name()
		if !strings.HasPrefix(name, "runtime.") {
			break
		}
	}

	return fmt.Sprintf("Panic occured at: %v:%v (line %v)", file, name, line)
}

func handleConnection(remoteConn net.Conn, l1, l2 handlers.Handler) {
	defer func() {
		if r := recover(); r != nil {
			if r != io.EOF {
				fmt.Println("Recovered from runtime panic:", r)
				fmt.Println("Panic location: ", identifyPanic())
			}
		}
	}()

	remoteReader := bufio.NewReader(remoteConn)
	remoteWriter := bufio.NewWriter(remoteConn)

	var reqParser common.RequestParser
	var responder common.Responder
	var reqType common.RequestType
	var request interface{}

	// A connection is either binary protocol or text. It cannot switch between the two.
	// This is the way memcached handles protocols, so it can be as strict here.
	binary, err := isBinaryRequest(remoteReader)
	if err != nil {
		abort([]io.Closer{remoteConn, l1, l2}, err, binary)
		return
	}

	if binary {
		reqParser = binprot.NewBinaryParser(remoteReader)
		responder = binprot.NewBinaryResponder(remoteWriter)
	} else {
		reqParser = textprot.NewTextParser(remoteReader)
		responder = textprot.NewTextResponder(remoteWriter)
	}

	for {
		request, reqType, err = reqParser.Parse()
		if err != nil {
			abort([]io.Closer{remoteConn, l1, l2}, err, binary)
			return
		}

		// TODO: handle nil
		switch reqType {
		case common.RequestSet:
			metrics.IncCounter(MetricCmdSet)
			req := request.(common.SetRequest)
			//fmt.Println("set", string(req.Key))

			metrics.IncCounter(MetricCmdSetL1)
			err = l1.Set(req, remoteReader)

			if err == nil {
				metrics.IncCounter(MetricCmdSetSuccessL1)
				// TODO: Account for L2
				metrics.IncCounter(MetricCmdSetSuccess)

				responder.Set()

			} else {
				metrics.IncCounter(MetricCmdSetErrorsL1)
				// TODO: Account for L2
				metrics.IncCounter(MetricCmdSetErrors)
			}

			// TODO: L2 metrics for sets, set success, set errors

		case common.RequestDelete:
			metrics.IncCounter(MetricCmdDelete)
			req := request.(common.DeleteRequest)
			//fmt.Println("delete", string(req.Key))

			metrics.IncCounter(MetricCmdDeleteL1)
			err = l1.Delete(req)

			if err == nil {
				metrics.IncCounter(MetricCmdDeleteHits)
				metrics.IncCounter(MetricCmdDeleteHitsL1)

				responder.Delete()

			} else if err == common.ErrKeyNotFound {
				metrics.IncCounter(MetricCmdDeleteMissesL1)
				// TODO: Account for L2
				metrics.IncCounter(MetricCmdDeleteMisses)
			} else {
				metrics.IncCounter(MetricCmdDeleteErrorsL1)
				// TODO: Account for L2
				metrics.IncCounter(MetricCmdDeleteErrors)
			}

			// TODO: L2 metrics for deletes, delete hits, delete misses, delete errors

		case common.RequestTouch:
			metrics.IncCounter(MetricCmdTouch)
			req := request.(common.TouchRequest)
			//fmt.Println("touch", string(req.Key))

			metrics.IncCounter(MetricCmdTouchL1)
			err = l1.Touch(req)

			if err == nil {
				metrics.IncCounter(MetricCmdTouchHitsL1)
				// TODO: Account for L2
				metrics.IncCounter(MetricCmdTouchHits)

				responder.Touch()

			} else if err == common.ErrKeyNotFound {
				metrics.IncCounter(MetricCmdTouchMissesL1)
				// TODO: Account for L2
				metrics.IncCounter(MetricCmdTouchMisses)
			} else {
				metrics.IncCounter(MetricCmdTouchMissesL1)
				// TODO: Account for L2
				metrics.IncCounter(MetricCmdTouchMisses)
			}

			// TODO: L2 metrics for touches, touch hits, touch misses, touch errors

		case common.RequestGet:
			start := time.Now()
			metrics.IncCounter(MetricCmdGet)
			req := request.(common.GetRequest)
			metrics.IncCounterBy(MetricCmdGetKeys, uint64(len(req.Keys)))
			//debugString := "get"
			//for _, k := range req.Keys {
			//	debugString += " "
			//	debugString += string(k)
			//}
			//println(debugString)

			metrics.IncCounter(MetricCmdGetL1)
			metrics.IncCounterBy(MetricCmdGetKeysL1, uint64(len(req.Keys)))
			resChan, errChan := l1.Get(req)

			// Read all the responses back from L1.
			// The contract is that the resChan will have GetResponse's for get hits and misses,
			// and the errChan will have any other errors, such as an out of memory error from
			// memcached. If any receive happens from errChan, there will be no more responses
			// from resChan.
			for {
				select {
				case res, ok := <-resChan:
					if !ok {
						resChan = nil
					} else {
						if res.Miss {
							metrics.IncCounter(MetricCmdGetHits)
							metrics.IncCounter(MetricCmdGetHitsL1)
							responder.GetMiss(res)
						} else {
							metrics.IncCounter(MetricCmdGetMissesL1)
							// TODO: Account for L2
							metrics.IncCounter(MetricCmdGetMisses)
							responder.Get(res)
						}
					}

				case getErr, ok := <-errChan:
					if !ok {
						errChan = nil
					} else {
						metrics.IncCounter(MetricCmdGetErrors)
						metrics.IncCounter(MetricCmdGetErrorsL1)
						err = getErr
					}
				}

				if resChan == nil && errChan == nil {
					break
				}
			}

			if err == nil {
				responder.GetEnd(req.NoopEnd)
			}

			metrics.ObserveHist(HistGet, uint64(time.Since(start)))

			// TODO: L2 metrics for gets, get hits, get misses, get errors

		case common.RequestGat:
			metrics.IncCounter(MetricCmdGat)
			req := request.(common.GATRequest)
			//fmt.Println("gat", string(req.Key))

			metrics.IncCounter(MetricCmdGatL1)
			res, err := l1.GAT(req)

			if err == nil {
				if res.Miss {
					metrics.IncCounter(MetricCmdGatMissesL1)
					// TODO: Account for L2
					metrics.IncCounter(MetricCmdGatMisses)
					responder.GATMiss(res)
				} else {
					metrics.IncCounter(MetricCmdGatHits)
					metrics.IncCounter(MetricCmdGatHitsL1)
					responder.GAT(res)
					responder.GetEnd(false)
				}
			} else {
				metrics.IncCounter(MetricCmdGatErrors)
				metrics.IncCounter(MetricCmdGatErrorsL1)
			}

			//TODO: L2 metrics for gats, gat hits, gat misses, gat errors

		case common.RequestUnknown:
			metrics.IncCounter(MetricCmdUnknown)
			err = common.ErrUnknownCmd
		}

		// TODO: distinguish fatal errors from non-fatal
		if err != nil {
			if common.IsAppError(err) {
				if err != common.ErrKeyNotFound {
					metrics.IncCounter(MetricErrAppError)
				}
				responder.Error(err)
			} else {
				metrics.IncCounter(MetricErrUnrecoverable)
				abort([]io.Closer{remoteConn, l1, l2}, err, binary)
				return
			}
		}
	}
}

func isBinaryRequest(reader *bufio.Reader) (bool, error) {
	headerByte, err := reader.Peek(1)
	if err != nil {
		return false, err
	}
	return int(headerByte[0]) == binprot.MagicRequest, nil
}
