package main

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"
)

type ChangeDelta struct {
	NewDefaultRoute string
}

type RouteUpdate struct {
	Added   []string
	Removed []string
}

type Shutdown struct {
	Reason string
}

type PublishedEvent struct {
	Event any
	From  *Client
}

type DeliveredEvent struct {
	Event any
	From  *Client
	To    *Client
}

type subscriber interface {
	subscribeType() reflect.Type
	dispatch(ctx context.Context, vals *queue[DeliveredEvent], acceptCh func() chan DeliveredEvent) bool
}

type subscriberCore struct {
	typ        reflect.Type
	dispatchFn func(ctx context.Context, vals *queue[DeliveredEvent], acceptCh func() chan DeliveredEvent) bool
}

func (c *subscriberCore) subscribeType() reflect.Type { return c.typ }
func (c *subscriberCore) dispatch(ctx context.Context, vals *queue[DeliveredEvent], acceptCh func() chan DeliveredEvent) bool {
	return c.dispatchFn(ctx, vals, acceptCh)
}

type Bus struct {
	mu     sync.Mutex
	topics map[reflect.Type][]*subscribeState
	write  chan PublishedEvent
	stop   context.CancelFunc
	done   chan struct{}
}

func NewBus() *Bus {
	ctx, stop := context.WithCancel(context.Background())
	b := &Bus{
		topics: make(map[reflect.Type][]*subscribeState),
		write:  make(chan PublishedEvent),
		stop:   stop,
		done:   make(chan struct{}),
	}
	go b.pump(ctx)
	return b
}

func (b *Bus) Close() {
	b.stop()
	<-b.done
}

func (b *Bus) pump(ctx context.Context) {
	defer close(b.done)
	for {
		select {
		case val := <-b.write:
			t := reflect.TypeOf(val.Event)
			b.mu.Lock()
			subs := b.topics[t]
			b.mu.Unlock()
			for _, ss := range subs {
				ss.write <- DeliveredEvent{Event: val.Event, From: val.From, To: ss.client}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (b *Bus) subscribe(t reflect.Type, ss *subscribeState) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.topics[t] = append(b.topics[t], ss)
}

type Client struct {
	name string
	bus  *Bus
	mu   sync.Mutex
	sub  *subscribeState
}

func (b *Bus) Client(name string) *Client {
	return &Client{name: name, bus: b}
}

func (c *Client) subscribeState() *subscribeState {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sub == nil {
		c.sub = newSubscribeState(c)
	}
	return c.sub
}

func (c *Client) Close() {
	c.mu.Lock()
	ss := c.sub
	c.sub = nil
	c.mu.Unlock()
	if ss != nil {
		ss.stop()
		<-ss.done
	}
}

type subscribeState struct {
	client  *Client
	write   chan DeliveredEvent
	done    chan struct{}
	stop    context.CancelFunc
	mu      sync.Mutex
	outputs map[reflect.Type]subscriber
}

func newSubscribeState(c *Client) *subscribeState {
	ctx, stop := context.WithCancel(context.Background())
	ss := &subscribeState{
		client:  c,
		write:   make(chan DeliveredEvent),
		done:    make(chan struct{}),
		stop:    stop,
		outputs: make(map[reflect.Type]subscriber),
	}
	go ss.pump(ctx)
	return ss
}

func (ss *subscribeState) pump(ctx context.Context) {
	defer close(ss.done)
	var vals queue[DeliveredEvent]
	acceptCh := func() chan DeliveredEvent {
		if vals.Full() {
			return nil
		}
		return ss.write
	}

	for {
		if !vals.Empty() {
			val := vals.Peek()
			ss.mu.Lock()
			sub := ss.outputs[reflect.TypeOf(val.Event)]
			ss.mu.Unlock()
			if sub == nil {
				vals.Drop()
				continue
			}
			if !sub.dispatch(ctx, &vals, acceptCh) {
				return
			}
		} else {
			select {
			case val := <-ss.write:
				vals.Add(val)
			case <-ctx.Done():
				return
			}
		}
	}
}

type Subscriber[T any] struct {
	core *subscriberCore
	ch   chan T
}

func (s *Subscriber[T]) dispatchTyped(ctx context.Context, vals *queue[DeliveredEvent], acceptCh func() chan DeliveredEvent) bool {
	t := vals.Peek().Event.(T)
	for {
		select {
		case s.ch <- t:
			vals.Drop()
			return true
		case val := <-acceptCh():
			vals.Add(val)
		case <-ctx.Done():
			return false
		}
	}
}

func Subscribe[T any](c *Client) *Subscriber[T] {
	t := reflect.TypeFor[T]()
	s := &Subscriber[T]{ch: make(chan T)}

	core := &subscriberCore{
		typ: t,
		dispatchFn: func(ctx context.Context, vals *queue[DeliveredEvent], acceptCh func() chan DeliveredEvent) bool {
			return s.dispatchTyped(ctx, vals, acceptCh)
		},
	}
	s.core = core

	ss := c.subscribeState()
	ss.mu.Lock()
	ss.outputs[t] = core
	ss.mu.Unlock()
	c.bus.subscribe(t, ss)
	return s
}

type SubscriberFunc[T any] struct {
	core *subscriberCore
}

func SubscribeFunc[T any](c *Client, f func(T)) *SubscriberFunc[T] {
	t := reflect.TypeFor[T]()
	core := &subscriberCore{
		typ: t,
		dispatchFn: func(ctx context.Context, vals *queue[DeliveredEvent], acceptCh func() chan DeliveredEvent) bool {
			// Only these two lines need T: unbox and call the user function.
			event := vals.Peek().Event.(T)
			callDone := make(chan struct{})
			go runFuncCallback(f, event, callDone)
			// The rest is non-generic.
			return dispatchFunc(ctx, vals, acceptCh, callDone)
		},
	}
	s := &SubscriberFunc[T]{core: core}

	ss := c.subscribeState()
	ss.mu.Lock()
	ss.outputs[t] = core
	ss.mu.Unlock()
	c.bus.subscribe(t, ss)
	return s
}

// runFuncCallback runs f(t) and closes done. Kept as a named function
// so go runFuncCallback(f, t, done) doesn't allocate a closure per event.
func runFuncCallback[T any](f func(T), t T, done chan struct{}) {
	defer close(done)
	f(t)
}

// dispatchFunc is the non-generic select loop shared by all SubscriberFunc[T].
// It doesn't need T — it just waits for the callback goroutine to finish.
func dispatchFunc(ctx context.Context, vals *queue[DeliveredEvent], acceptCh func() chan DeliveredEvent, callDone chan struct{}) bool {
	for {
		select {
		case <-callDone:
			vals.Drop()
			return true
		case val := <-acceptCh():
			vals.Add(val)
		case <-ctx.Done():
			return false
		}
	}
}

func Publish[T any](c *Client, v T) {
	c.bus.write <- PublishedEvent{Event: v, From: c}
}

func main() {
	b := NewBus()
	netmon := b.Client("netmon")

	// With Subscribe: you manage the goroutine and read loop yourself.
	dialer := b.Client("tsdial")
	netSub := Subscribe[ChangeDelta](dialer)
	go func() {
		for cd := range netSub.ch {
			fmt.Printf("tsdial: refreshing connections for %s\n", cd.NewDefaultRoute)
		}
	}()

	// With SubscribeFunc: just pass a function. No goroutine, no loop.
	backend := b.Client("ipnlocal")
	SubscribeFunc[ChangeDelta](backend, func(cd ChangeDelta) {
		fmt.Printf("ipnlocal: updated route to %s\n", cd.NewDefaultRoute)
	})

	go func() {
		Publish(netmon, ChangeDelta{NewDefaultRoute: "192.168.1.1"})
	}()

	time.Sleep(50 * time.Millisecond)
	dialer.Close()
	backend.Close()
	netmon.Close()
	b.Close()
}
