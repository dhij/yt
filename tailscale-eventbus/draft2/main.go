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

type Bus struct {
	mu     sync.Mutex
	topics map[reflect.Type][]any
	write  chan any
}

func NewBus() *Bus {
	b := &Bus{
		topics: make(map[reflect.Type][]any),
		write:  make(chan any),
	}
	go b.pump()
	return b
}

func (b *Bus) pump() {
	for val := range b.write {
		t := reflect.TypeOf(val)
		b.mu.Lock()
		subs := b.topics[t]
		b.mu.Unlock()
		for _, s := range subs {
			// DOES NOT COMPILE: T doesn't exist in this scope.
			// We need: s.(*Subscriber[T]).ch <- val.(T)
			// but there is no T to write here.
			_ = s
			_ = val
			// s.(*Subscriber[???]).ch <- val
		}
	}
}

type Subscriber[T any] struct {
	ch chan T
}

func Subscribe[T any](b *Bus) *Subscriber[T] {
	s := &Subscriber[T]{ch: make(chan T, 16)}
	b.mu.Lock()
	b.topics[reflect.TypeFor[T]()] = append(b.topics[reflect.TypeFor[T]()], s)
	b.mu.Unlock()
	return s
}

func Publish[T any](b *Bus, v T) {
	b.write <- v
}

func main() {
	b := NewBus()
	_ = Subscribe[ChangeDelta](b)
	_ = Subscribe[RouteUpdate](b)

	go func() {
		Publish(b, ChangeDelta{NewDefaultRoute: "192.168.1.1"})
		Publish(b, RouteUpdate{Added: []string{"10.0.0.0/8"}})
	}()

	// Can't receive — pump doesn't deliver. This is the broken draft.
	fmt.Println("draft2: pump can't deliver because it doesn't know T")
}
