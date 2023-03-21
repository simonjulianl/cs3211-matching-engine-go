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
}

func getInitEngine(ctx context.Context) *Engine {
	e := &Engine{
		clientCh:          make(chan Order),
		instrumentMapping: make(map[string]chan<- Order),
	}

	go e.createDistributor(ctx)
	return e
}
func (e *Engine) createDistributor(ctx context.Context) {
	for {
		select {
		case o := <-e.clientCh:
			// get channel
			ch := e.instrumentMapping[o.instrument]
			if ch == nil {
				instCh := make(chan Order, WorkerBufferSize)
				e.instrumentMapping[o.instrument] = instCh

				// create the worker per instrument
				w := getWorker(instCh)
				go w.work(ctx)

				ch = instCh
			}

			ch <- o
		case <-ctx.Done():
			return
		}
	}
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
			timestamp:   GetCurrentTimestamp(),
			count:       in.count,
			executionId: 0,
			instrument:  in.instrument,
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
		// TODO: Example of adding orders
		// outputOrderAdded(in, GetCurrentTimestamp())
		// outputOrderDeleted(in, true, GetCurrentTimestamp())
		// outputOrderExecuted(123, 124, 1, 2000, 10, GetCurrentTimestamp())
	}
}

func GetCurrentTimestamp() int64 {
	return time.Now().UnixNano()
}
