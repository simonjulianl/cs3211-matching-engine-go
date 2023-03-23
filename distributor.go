package main

import "context"

type Distributor struct {
	clientCh          <-chan Order
	deletionCh        chan uint32
	instrumentMapping map[string]chan<- Order
	idMapping         map[uint32]string
}

func initDistributor(distributorChannel <-chan Order, ctx context.Context) {
	d := &Distributor{
		clientCh:          distributorChannel,
		deletionCh:        make(chan uint32, WorkerBufferSize),
		instrumentMapping: make(map[string]chan<- Order),
		idMapping:         make(map[uint32]string),
	}

	go d.start(ctx)
}

func (d *Distributor) start(ctx context.Context) {
	for {
		select {
		case id := <-d.deletionCh:
			delete(d.idMapping, id)
		case o := <-d.clientCh:
			if o.instrument == "" { // cancel order
				if val, ok := d.idMapping[o.orderId]; ok {
					o.instrument = val
				} else {
					outputOrderDeleted(o, false, GetCurrentTimestamp())
					o.done <- struct{}{}
					break
				}
			}

			if o.input != inputCancel {
				d.idMapping[o.orderId] = o.instrument
			}

			if val, ok := d.instrumentMapping[o.instrument]; !ok {
				d.createWorkerAndSend(ctx, o)
			} else {
				val <- o
			}
		case <-ctx.Done():
			return
		}
	}
}

func (d *Distributor) createWorkerAndSend(ctx context.Context, o Order) {
	instCh := make(chan Order, WorkerBufferSize)
	d.instrumentMapping[o.instrument] = instCh

	// create the worker per instrument
	initWorker(ctx, instCh, d.deletionCh)
	instCh <- o
}
