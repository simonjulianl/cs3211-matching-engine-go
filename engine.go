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
	distributorCh chan<- Order
}

func getInitEngine(ctx context.Context) *Engine {
	distributorChannel := make(chan Order, WorkerBufferSize)
	e := &Engine{distributorCh: distributorChannel}
	initDistributor(distributorChannel, ctx)
	return e
}

func (e *Engine) accept(ctx context.Context, conn net.Conn) {
	go func() {
		<-ctx.Done()
		conn.Close()
	}()
	go handleConn(conn, e.distributorCh)
}

// client connection
func handleConn(conn net.Conn, distributorCh chan<- Order) {
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
			orderId:       in.orderId,
			price:         in.price,
			count:         in.count,
			executionId:   0,
			instrument:    in.instrument,
			done:          done,
			insertRequest: false,
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
