package main

import (
	"container/heap"
	"context"
)

type BuyWorker struct { // should have used interface
	inputCh               <-chan Order
	sellOutputCh          chan<- Order
	deletionDistributorCh chan<- uint32
	deletionWorkerCh      chan<- uint32
	done                  chan<- struct{}
	sellOrderbook         *Orderbook
	orderIdMapping        map[uint32]*Order
}

func (bw *BuyWorker) work(ctx context.Context) {
	for {
		select {
		case o := <-bw.inputCh:
			if o.insertRequest {
				bw.handleInsertion(o)
				o.done <- struct{}{}
				break
			}

			switch o.input {
			case inputCancel:
				bw.handleCancel(o)
				o.done <- struct{}{}
			case inputBuy:
				bw.handleBuy(o)
			default: // not recognized input type
				o.printOrder()
			}
			bw.done <- struct{}{}
		case <-ctx.Done():
			return
		}
	}
}
func (bw *BuyWorker) handleInsertion(o Order) {
	refO := &o
	refO.timestamp = GetCurrentTimestamp()

	bw.orderIdMapping[refO.orderId] = refO
	heap.Push(bw.sellOrderbook, refO)
	outputOrderAdded(o, refO.timestamp)
}

func (bw *BuyWorker) handleCancel(o Order) {
	bw.deletionWorkerCh <- o.orderId
	bw.deletionDistributorCh <- o.orderId

	if val, ok := bw.orderIdMapping[o.orderId]; ok {
		val.count = 0
		outputOrderDeleted(o, true, GetCurrentTimestamp())
		delete(bw.orderIdMapping, o.orderId)
	} else {
		outputOrderDeleted(o, false, GetCurrentTimestamp())
	}
}
func (bw *BuyWorker) handleBuy(o Order) {
	for bw.sellOrderbook.Len() > 0 && o.count > 0 {
		topSellOrder := bw.sellOrderbook.Top()
		if topSellOrder.count == 0 {
			heap.Pop(bw.sellOrderbook) // deleted order
			continue
		}

		if isOrderMatching(o.price, topSellOrder.price) {
			qtyMatching := min(o.count, topSellOrder.count)
			o.count -= qtyMatching
			topSellOrder.count -= qtyMatching
			topSellOrder.executionId += 1

			outputOrderExecuted(
				topSellOrder.orderId,
				o.orderId,
				topSellOrder.executionId,
				topSellOrder.price,
				qtyMatching,
				GetCurrentTimestamp(),
			)

			if topSellOrder.count == 0 {
				delete(bw.orderIdMapping, topSellOrder.orderId)
				bw.deletionDistributorCh <- topSellOrder.orderId
				bw.deletionWorkerCh <- topSellOrder.orderId
				heap.Pop(bw.sellOrderbook)
			}
		} else {
			break
		}
	}

	if o.count > 0 {
		o.insertRequest = true
		bw.sellOutputCh <- o
	} else {
		o.done <- struct{}{}
	}
}
