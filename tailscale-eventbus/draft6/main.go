package main

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

type ChangeDelta struct {
	NewDefaultRoute string
}

type RouteUpdate struct {
	Added   []string
	Removed []string
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

// subscriber is the interface subscribeState uses to dispatch events.
type subscriber interface {
	subscribeType() reflect.Type
	dispatch(ctx context.Context, vals *queue[DeliveredEvent], acceptCh func() chan DeliveredEvent) bool
}

// subscriberCore implements subscriber once for all T.
// One itab, shared across every event type.
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

// Subscriber[T] is the thin generic facade. It owns the typed channel.
type Subscriber[T any] struct {
	core *subscriberCore
	ch   chan T
}

// dispatchTyped is the per-T select loop. Renamed from dispatch (Draft 5)
// because subscriberCore.dispatch now owns the interface method name.
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

	// The closure captures s (*Subscriber[T]), so it knows T.
	// But interface dispatch goes through the non-generic subscriberCore.
	core := &subscriberCore{
		typ: t,
		dispatchFn: func(ctx context.Context, vals *queue[DeliveredEvent], acceptCh func() chan DeliveredEvent) bool {
			return s.dispatchTyped(ctx, vals, acceptCh)
		},
	}
	s.core = core

	ss := c.subscribeState()
	ss.mu.Lock()
	ss.outputs[t] = core // register the non-generic core, not Subscriber[T]
	ss.mu.Unlock()
	c.bus.subscribe(t, ss)
	return s
}

func Publish[T any](c *Client, v T) {
	c.bus.write <- PublishedEvent{Event: v, From: c}
}

func main() {
	b := NewBus()

	netmon := b.Client("netmon")

	backend := b.Client("ipnlocal")
	netSub := Subscribe[ChangeDelta](backend)
	routeSub := Subscribe[RouteUpdate](backend)

	go func() {
		Publish(netmon, ChangeDelta{NewDefaultRoute: "192.168.1.1"})
		Publish(netmon, RouteUpdate{Added: []string{"10.0.0.0/8"}})
	}()

	cd := <-netSub.ch
	fmt.Printf("network changed: new route %s\n", cd.NewDefaultRoute)

	ru := <-routeSub.ch
	fmt.Printf("routes updated: added %v\n", ru.Added)

	backend.Close()
	netmon.Close()
	b.Close()
}
