package main

import "C"
import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

const WorkerBufferSize = 100

type Engine struct {
	clientCh          chan Order
	instrumentMapping map[string]chan<- Order
	idMapping         map[uint32]string
}

func getInitEngine(ctx context.Context) *Engine {
	e := &Engine{
		clientCh:          make(chan Order),
		instrumentMapping: make(map[string]chan<- Order),
		idMapping:         make(map[uint32]string),
	}

	go e.createDistributor(ctx)
	return e
}
func (e *Engine) createDistributor(ctx context.Context) {
	for {
		select {
		case o := <-e.clientCh:
			if o.instrument == "" { // cancel order
				if val, ok := e.idMapping[o.orderId]; ok {
					o.instrument = val
				} else {
					// something is wrong, we are guaranteed that the order for cancellation is valid
					o.printOrder()
				}
			}

			e.idMapping[o.orderId] = o.instrument
			if val, ok := e.instrumentMapping[o.instrument]; !ok {
				e.createWorkerAndSend(ctx, o)
			} else {
				val <- o
			}
		case <-ctx.Done():
			return
		}
	}
}

func (e *Engine) createWorkerAndSend(ctx context.Context, o Order) {
	instCh := make(chan Order, WorkerBufferSize)
	e.instrumentMapping[o.instrument] = instCh

	// create the worker per instrument
	w := getWorker(instCh)
	go w.work(ctx)
	instCh <- o
}

func (e *Engine) accept(ctx context.Context, conn net.Conn) {
	go func() {
		<-ctx.Done()
		conn.Close()
	}()
	go handleConn(conn, e.clientCh)
}

// client connection
func handleConn(conn net.Conn, distributorCh chan Order) {
	defer conn.Close()
	done := make(chan struct{})
	for {
		in, err := readInput(conn)
		if err != nil {
			if err != io.EOF {
				_, _ = fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			}
			return
		}
		o := Order{
			orderId:     in.orderId,
			price:       in.price,
			count:       in.count,
			executionId: 0,
			instrument:  in.instrument,
			done:        done,
		}
		switch in.orderType {
		case inputBuy:
			o.input = inputBuy
		case inputSell:
			o.input = inputSell
		case inputCancel:
			o.input = inputCancel
		}
		distributorCh <- o
		<-done
	}
}

func GetCurrentTimestamp() int64 {
	return time.Now().UnixNano()
}
