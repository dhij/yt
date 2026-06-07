package main

import (
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

type Bus struct {
	mu     sync.Mutex
	topics map[reflect.Type][]*subscribeState
	write  chan PublishedEvent
	done   chan struct{}
}

func NewBus() *Bus {
	b := &Bus{
		topics: make(map[reflect.Type][]*subscribeState),
		write:  make(chan PublishedEvent),
		done:   make(chan struct{}),
	}
	go b.pump()
	return b
}

func (b *Bus) Close() {
	close(b.write)
	<-b.done
}

func (b *Bus) pump() {
	defer close(b.done)
	for val := range b.write {
		t := reflect.TypeOf(val.Event)
		b.mu.Lock()
		subs := b.topics[t]
		b.mu.Unlock()
		for _, ss := range subs {
			ss.write <- DeliveredEvent{Event: val.Event, From: val.From, To: ss.client}
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
		close(ss.write)
		<-ss.done
	}
}

// subscriber is the interface subscribeState uses to deliver events.
// subscribeState is non-generic, so it needs an interface to call
// through to the right Subscriber[T].
type subscriber interface {
	send(event any)
}

// subscribeState is per-client. It receives events of different types
// on a single channel and routes them to the right Subscriber[T].
type subscribeState struct {
	client  *Client
	write   chan DeliveredEvent
	done    chan struct{}
	mu      sync.Mutex
	outputs map[reflect.Type]subscriber // maps event type to the Subscriber[T]
}

func newSubscribeState(c *Client) *subscribeState {
	ss := &subscribeState{
		client:  c,
		write:   make(chan DeliveredEvent),
		done:    make(chan struct{}),
		outputs: make(map[reflect.Type]subscriber),
	}
	go ss.pump()
	return ss
}

// pump is the per-client event loop. Events of all types are delivered
// one at a time, in publication order.
func (ss *subscribeState) pump() {
	defer close(ss.done)
	for val := range ss.write {
		t := reflect.TypeOf(val.Event)
		ss.mu.Lock()
		out := ss.outputs[t]
		ss.mu.Unlock()
		if out != nil {
			out.send(val.Event) // blocks if caller is slow
		}
	}
}

type Subscriber[T any] struct {
	ch chan T
}

func (s *Subscriber[T]) send(event any) {
	s.ch <- event.(T) // unbox and send — blocks if caller is slow
}

func Subscribe[T any](c *Client) *Subscriber[T] {
	t := reflect.TypeFor[T]()
	s := &Subscriber[T]{ch: make(chan T)}

	ss := c.subscribeState()
	ss.mu.Lock()
	ss.outputs[t] = s
	ss.mu.Unlock()
	c.bus.subscribe(t, ss)
	return s
}

func Publish[T any](c *Client, v T) {
	c.bus.write <- PublishedEvent{Event: v, From: c}
}

func main() {
	b := NewBus()

	// netmon publishes network changes
	netmon := b.Client("netmon")

	// ipnlocal subscribes to both network changes and route updates
	backend := b.Client("ipnlocal")
	netSub := Subscribe[ChangeDelta](backend)
	routeSub := Subscribe[RouteUpdate](backend)

	go func() {
		Publish(netmon, ChangeDelta{NewDefaultRoute: "192.168.1.1"})
		Publish(netmon, RouteUpdate{Added: []string{"10.0.0.0/8"}})
	}()

	// Both events delivered in publication order through a single client.
	cd := <-netSub.ch
	fmt.Printf("network changed: new route %s\n", cd.NewDefaultRoute)

	ru := <-routeSub.ch
	fmt.Printf("routes updated: added %v\n", ru.Added)

	backend.Close()
	netmon.Close()
	b.Close()
}
