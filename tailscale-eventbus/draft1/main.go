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
}

func NewBus() *Bus {
	return &Bus{topics: make(map[reflect.Type][]any)}
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
	b.mu.Lock()
	subs := b.topics[reflect.TypeFor[T]()]
	b.mu.Unlock()
	for _, s := range subs {
		s.(*Subscriber[T]).ch <- v
	}
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
}
