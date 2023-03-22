package main

import (
	"context"
)

type Worker struct {
	inputCh            <-chan Order
	buyOutputCh        chan<- Order
	buyDone            chan struct{}
	currentBuy         *Order // buy worker handles sell order
	sellOutputCh       chan<- Order
	sellDone           chan struct{}
	currentSell        *Order // sell worker handles buy order
	deletionWorkerCh   <-chan uint32
	orderIdTypeMapping map[uint32]inputType
}

func initWorker(ctx context.Context, inputCh <-chan Order, deletionCh chan<- uint32) {
	buyCh := make(chan Order)
	buyDone := make(chan struct{}, 1)
	buyDone <- struct{}{}

	sellCh := make(chan Order)
	sellDone := make(chan struct{}, 1)
	sellDone <- struct{}{}

	deletionWorkerCh := make(chan uint32, WorkerBufferSize)

	w := &Worker{
		inputCh:            inputCh,
		buyOutputCh:        buyCh,
		buyDone:            buyDone,
		currentBuy:         nil,
		sellOutputCh:       sellCh,
		sellDone:           sellDone,
		currentSell:        nil,
		deletionWorkerCh:   deletionWorkerCh,
		orderIdTypeMapping: make(map[uint32]inputType),
	}

	bw := &BuyWorker{
		inputCh:               buyCh,
		sellOutputCh:          sellCh,
		deletionDistributorCh: deletionCh,
		deletionWorkerCh:      deletionWorkerCh,
		done:                  buyDone,
		sellOrderbook:         getSellOrderbook(),
		orderIdMapping:        make(map[uint32]*Order),
	}

	sw := &SellWorker{
		inputCh:               sellCh,
		buyOutputCh:           buyCh,
		deletionDistributorCh: deletionCh,
		deletionWorkerCh:      deletionWorkerCh,
		done:                  sellDone,
		buyOrderbook:          getBuyOrderbook(),
		orderIdMapping:        make(map[uint32]*Order),
	}

	go bw.work(ctx)
	go sw.work(ctx)
	go w.work(ctx)
}
func (w *Worker) work(ctx context.Context) {
	for {
		select {
		case id := <-w.deletionWorkerCh:
			delete(w.orderIdTypeMapping, id)
		case o := <-w.inputCh:
			switch o.input {
			case inputCancel:
				/*
					Realize that you cannot have concurrent delete and (buy/sell). This is because it is guaranteed that
					the deletion comes from the same thread which must wait until the current order to be finished.
				*/
				w.handleCancel(o)
			case inputBuy:
				w.handleSafeBuy(o)
			case inputSell:
				w.handleSafeSell(o)
			default: // not recognized input type
				o.printOrder()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) handleSafeBuy(o Order) {
	<-w.buyDone // no one is buying
	w.currentBuy = &o
	w.orderIdTypeMapping[o.orderId] = inputSell // buy order will be put in buy orderbook in sell worker
	w.buyOutputCh <- o
	<-w.buyDone // ensure that the processing is done
	w.currentBuy = nil
	w.buyDone <- struct{}{}
}

func (w *Worker) handleSafeSell(o Order) {
	<-w.sellDone // no one in selling
	w.currentSell = &o
	w.orderIdTypeMapping[o.orderId] = inputBuy // sell order will be put in sell orderbook in buy worker
	w.sellOutputCh <- o
	<-w.sellDone // ensure that the sell has been done
	w.currentSell = nil
	w.sellDone <- struct{}{}
}

func (w *Worker) handleCancel(o Order) {
	if val, ok := w.orderIdTypeMapping[o.orderId]; ok {
		delete(w.orderIdTypeMapping, o.orderId)
		if val == inputBuy {
			<-w.buyDone
			w.buyOutputCh <- o
		} else {
			// sell
			<-w.sellDone
			w.sellOutputCh <- o
		}
	} else { // already deleted, not in buy or sell anymore
		outputOrderDeleted(o, false, GetCurrentTimestamp())
		o.done <- struct{}{}
	}
}

// ========= UTILITY =================
func isOrderMatching(buyPrice uint32, sellPrice uint32) bool {
	return buyPrice >= sellPrice
}
func min(a uint32, b uint32) uint32 {
	if a > b {
		return b
	} else {
		return a
	}
}
