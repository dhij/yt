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
}

type DeliveredEvent struct {
	Event any
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
		for _, s := range subs {
			s.write <- DeliveredEvent{Event: val.Event}
		}
	}
}

type subscribeState struct {
	write chan DeliveredEvent
}

type Subscriber[T any] struct {
	state *subscribeState
	ch    chan T
}

func Subscribe[T any](b *Bus) *Subscriber[T] {
	state := &subscribeState{
		write: make(chan DeliveredEvent),
	}
	s := &Subscriber[T]{
		state: state,
		ch:    make(chan T),
	}
	b.mu.Lock()
	b.topics[reflect.TypeFor[T]()] = append(b.topics[reflect.TypeFor[T]()], state)
	b.mu.Unlock()

	go s.pump()
	return s
}

func (s *Subscriber[T]) pump() {
	for val := range s.state.write {
		s.ch <- val.Event.(T)
	}
}

func Publish[T any](b *Bus, v T) {
	b.write <- PublishedEvent{Event: v}
}

func main() {
	b := NewBus()

	netSub := Subscribe[ChangeDelta](b)
	routeSub := Subscribe[RouteUpdate](b)

	go func() {
		Publish(b, ChangeDelta{NewDefaultRoute: "192.168.1.1"})
		Publish(b, RouteUpdate{Added: []string{"10.0.0.0/8"}})
	}()

	cd := <-netSub.ch
	fmt.Printf("network changed: new route %s\n", cd.NewDefaultRoute)

	ru := <-routeSub.ch
	fmt.Printf("routes updated: added %v\n", ru.Added)

	b.Close()
}
