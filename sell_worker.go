package main

import (
	"container/heap"
	"context"
)

type SellWorker struct {
	inputCh               <-chan Order
	buyOutputCh           chan<- Order
	deletionDistributorCh chan<- uint32
	deletionWorkerCh      chan<- uint32
	done                  chan<- struct{}
	buyOrderbook          *Orderbook
	orderIdMapping        map[uint32]*Order
}

func (sw *SellWorker) work(ctx context.Context) {
	for {
		select {
		case o := <-sw.inputCh:
			if o.insertRequest {
				sw.handleInsertion(o)
				o.done <- struct{}{}
				break
			}

			switch o.input {
			case inputCancel:
				sw.handleCancel(o)
				o.done <- struct{}{}
			case inputSell:
				sw.handleSell(o)
			default: // not recognized input type
				o.printOrder()
			}
			sw.done <- struct{}{}
		case <-ctx.Done():
			return
		}
	}
}

func (sw *SellWorker) handleInsertion(o Order) {
	refO := &o
	refO.timestamp = GetCurrentTimestamp()

	sw.orderIdMapping[refO.orderId] = refO
	heap.Push(sw.buyOrderbook, refO)
	outputOrderAdded(o, refO.timestamp)
}

func (sw *SellWorker) handleCancel(o Order) {
	sw.deletionWorkerCh <- o.orderId
	sw.deletionDistributorCh <- o.orderId

	if val, ok := sw.orderIdMapping[o.orderId]; ok {
		val.count = 0
		outputOrderDeleted(o, true, GetCurrentTimestamp())
		delete(sw.orderIdMapping, o.orderId)
	} else {
		outputOrderDeleted(o, false, GetCurrentTimestamp())
	}
}
func (sw *SellWorker) handleSell(o Order) {
	for sw.buyOrderbook.Len() > 0 && o.count > 0 {
		topBuyOrder := sw.buyOrderbook.Top()
		if topBuyOrder.count == 0 {
			heap.Pop(sw.buyOrderbook) // deleted order
			continue
		}

		if isOrderMatching(topBuyOrder.price, o.price) {
			qtyMatching := min(o.count, topBuyOrder.count)
			o.count -= qtyMatching
			topBuyOrder.count -= qtyMatching
			topBuyOrder.executionId += 1

			outputOrderExecuted(
				topBuyOrder.orderId,
				o.orderId,
				topBuyOrder.executionId,
				topBuyOrder.price,
				qtyMatching,
				GetCurrentTimestamp(),
			)

			if topBuyOrder.count == 0 {
				delete(sw.orderIdMapping, topBuyOrder.orderId)
				sw.deletionDistributorCh <- topBuyOrder.orderId
				sw.deletionWorkerCh <- topBuyOrder.orderId
				heap.Pop(sw.buyOrderbook)
			}
		} else {
			break
		}
	}

	if o.count > 0 {
		o.insertRequest = true
		sw.buyOutputCh <- o
	} else {
		o.done <- struct{}{}
	}
}
