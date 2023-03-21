package main

import (
	"context"
)

type Worker struct {
	inputCh        <-chan Order
	buyOrderbook   *Orderbook
	sellOrderbook  *Orderbook
	orderIdMapping map[uint32]*Order
}

func getWorker(inputCh <-chan Order) *Worker {
	return &Worker{
		inputCh:        inputCh,
		buyOrderbook:   getBuyOrderbook(),
		sellOrderbook:  getSellOrderbook(),
		orderIdMapping: make(map[uint32]*Order),
	}
}
func (w *Worker) work(ctx context.Context) {
	for {
		select {
		case o := <-w.inputCh:
			o.printOrder()
		case <-ctx.Done():
			return
		}
	}
}
