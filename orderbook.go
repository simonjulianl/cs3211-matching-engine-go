package main

import (
	"fmt"
)

type Order struct {
	price       uint32
	timestamp   int64
	count       uint32
	orderId     uint32
	executionId uint32
	input       inputType
	instrument  string
}

func (o *Order) printOrder() {
	fmt.Printf("%+v\n", o)
}

type LessFunction func(i, j int, data []*Order) bool

// An Orderbook implements heap.Interface and holds Order.
type Orderbook struct {
	lesser LessFunction
	data   []*Order
}

func GetOrderbook(l LessFunction) *Orderbook {
	return &Orderbook{lesser: l, data: make([]*Order, 0)}
}

func (ob *Orderbook) Len() int { return len(ob.data) }

func (ob *Orderbook) Less(i, j int) bool {
	return ob.lesser(i, j, ob.data)
}

func (ob *Orderbook) Swap(i, j int) {
	ob.data[i], ob.data[j] = ob.data[j], ob.data[i]
}

func (ob *Orderbook) Push(x any) {
	item := x.(*Order)
	ob.data = append(ob.data, item)
}

func (ob *Orderbook) Pop() any {
	old := ob.data
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	ob.data = old[0 : n-1]
	return item
}

func (ob *Orderbook) Top() *Order {
	if len(ob.data) > 0 {
		return ob.data[0]
	}

	return nil
}

func getBuyOrderbook() *Orderbook {
	comp := func(i, j int, data []*Order) bool {
		firstOrder := data[i]
		secondOrder := data[j]
		if firstOrder.price == secondOrder.price {
			return firstOrder.timestamp < secondOrder.timestamp
		}
		return firstOrder.price > secondOrder.price
	}
	return GetOrderbook(comp)
}

func getSellOrderbook() *Orderbook {
	comp := func(i, j int, data []*Order) bool {
		firstOrder := data[i]
		secondOrder := data[j]
		if firstOrder.price == secondOrder.price {
			return firstOrder.timestamp < secondOrder.timestamp
		}
		return firstOrder.price < secondOrder.price
	}
	return GetOrderbook(comp)
}

// uncomment to run the main since there can only be 1 main per package
/*
func main() { // run using go run orderbook.go
	fmt.Println("====BUY ORDERBOOK=====")
	q := getBuyOrderbook()
	heap.Init(q)
	o0 := &Order{
		price:     1,
		timestamp: 1,
	}
	o1 := &Order{
		price:     2,
		timestamp: 1,
	}
	o2 := &Order{
		price:     2,
		timestamp: 2,
	}

	heap.Push(q, o0)
	heap.Push(q, o1)
	heap.Push(q, o2)
	o2.count = 10 // attempt to modify the pointer

	for q.Len() > 0 {
		item := heap.Pop(q).(*Order)
		item.printOrder()
	}

	fmt.Println("====SELL ORDERBOOK=====")
	sq := getSellOrderbook()
	heap.Init(sq)
	o3 := &Order{
		price:     0,
		timestamp: 1,
	}
	o4 := &Order{
		price:     2,
		timestamp: 1,
	}
	o5 := &Order{
		price:     2,
		timestamp: 2,
	}

	heap.Push(sq, o3)
	heap.Push(sq, o4)
	heap.Push(sq, o5)
	sq.Top().count = 100

	for sq.Len() > 0 {
		item := heap.Pop(sq).(*Order)
		item.printOrder()
	}
}
*/
