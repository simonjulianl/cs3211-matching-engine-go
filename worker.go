package main

import (
	"container/heap"
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
			switch o.input {
			case inputCancel:
				w.handleCancel(o)
			case inputBuy:
				w.handleBuy(o)
			case inputSell:
				w.handleSell(o)
			default: // not recognized input type
				o.printOrder()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) handleCancel(o Order) {
	if val, ok := w.orderIdMapping[o.orderId]; ok {
		val.count = 0
		outputOrderDeleted(o, true, GetCurrentTimestamp())
		delete(w.orderIdMapping, o.orderId)
	} else {
		outputOrderDeleted(o, false, GetCurrentTimestamp())
	}
}

func (w *Worker) isOrderMatching(buyPrice uint32, sellPrice uint32) bool {
	return buyPrice >= sellPrice
}
func (w *Worker) handleBuy(o Order) {
	for w.sellOrderbook.Len() > 0 && o.count > 0 {
		topSellOrder := w.sellOrderbook.Top()
		if topSellOrder.count == 0 {
			heap.Pop(w.sellOrderbook) // deleted order
			continue
		}

		if w.isOrderMatching(o.price, topSellOrder.price) {
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
				heap.Pop(w.sellOrderbook)
			}
		} else {
			break
		}
	}

	if o.count > 0 {
		refO := &o
		refO.timestamp = GetCurrentTimestamp()

		w.orderIdMapping[refO.orderId] = refO
		heap.Push(w.buyOrderbook, refO)
		outputOrderAdded(o, refO.timestamp)
	}
}

func (w *Worker) handleSell(o Order) {
	for w.buyOrderbook.Len() > 0 && o.count > 0 {
		topBuyOrder := w.buyOrderbook.Top()
		if topBuyOrder.count == 0 {
			heap.Pop(w.buyOrderbook) // deleted order
			continue
		}

		if w.isOrderMatching(topBuyOrder.price, o.price) {
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
				heap.Pop(w.buyOrderbook)
			}
		} else {
			break
		}
	}

	if o.count > 0 {
		refO := &o
		refO.timestamp = GetCurrentTimestamp()

		w.orderIdMapping[refO.orderId] = refO
		heap.Push(w.sellOrderbook, refO)
		outputOrderAdded(o, refO.timestamp)
	}
}

func min(a uint32, b uint32) uint32 {
	if a > b {
		return b
	} else {
		return a
	}
}
