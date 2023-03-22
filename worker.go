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
	buyCh := make(chan Order, WorkerBufferSize)
	buyDone := make(chan struct{}, 1)
	buyDone <- struct{}{}

	sellCh := make(chan Order, WorkerBufferSize)
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
					Realize that you cannot have concurrent delete and (buy/sell) for the same id.
					This is because it is guaranteed that the deletion comes from the same thread
					which must wait until the current order to be finished.
				*/
				w.handleCancel(o)
			case inputBuy:
				<-w.buyDone // no one is buying
				select {
				case <-w.sellDone:
					w.currentSell = nil
					w.sellDone <- struct{}{}
				default:
				}
				w.currentBuy = &o
				w.handleSafeBuy(o)
			case inputSell:
				<-w.sellDone // no one in selling
				select {
				case <-w.buyDone:
					w.currentBuy = nil
					w.buyDone <- struct{}{}
				default:
				}
				w.currentSell = &o
				w.handleSafeSell(o)
			default: // not recognized input type
				o.printOrder()
			}
		case <-ctx.Done():
			return
		}
	}
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

func (w *Worker) handleSafeBuy(o Order) {
	w.orderIdTypeMapping[o.orderId] = inputSell // buy order will be put in buy orderbook in sell worker
	if w.currentSell == nil {
		// we can immediately send the order
		w.buyOutputCh <- o
	} else {
		// the current sell is not nil, we need to check if we can
		if isOrderMatching(o.price, w.currentSell.price) {
			<-w.sellDone // wait until the sell is done
			w.buyOutputCh <- o
			w.sellDone <- struct{}{} // release the sell worker, allow it to work again
		} else { // order not matching, no harm in sending directly
			w.buyOutputCh <- o
		}
	}
}
func (w *Worker) handleSafeSell(o Order) {
	w.orderIdTypeMapping[o.orderId] = inputBuy // sell order will be put in sell orderbook in buy worker
	if w.currentBuy == nil {
		// we can immediately send the order
		w.sellOutputCh <- o
	} else {
		// the current sell is not nil, we need to check if we can
		if isOrderMatching(w.currentBuy.price, o.price) {
			<-w.buyDone // wait until the buy is done
			w.sellOutputCh <- o
			w.buyDone <- struct{}{} // release the buy worker, allow it to work again
		} else { // order not matching, no harm in sending directly
			w.sellOutputCh <- o
		}
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
