package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	timeoutParam = "timeout"
	msgParam     = "v"
)

type Node struct {
	data string
	next *Node
}

type Queue struct {
	mu    sync.Mutex
	first *Node
	last  *Node
}

type Broker struct {
	m  map[string]*Queue
	mu sync.RWMutex
}

type Endpoint struct {
	broker Broker
}

func (q *Queue) Push(msg string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	node := &Node{data: msg}
	if q.first == nil {
		q.first = node
		q.last = q.first
	} else {
		q.last.next = node
		q.last = node
	}
}

func (q *Queue) Pop() string {
	q.mu.Lock()
	defer q.mu.Unlock()

	node := q.first
	if q.first != nil {
		q.first = q.first.next
	}
	if q.first == nil {
		q.last = nil
	}

	var data string
	if node != nil {
		data = node.data
	}
	return data
}

func (b *Broker) PutMsg(queueName, data string) {
	b.mu.Lock()
	queue, ok := b.m[queueName]
	if !ok {
		queue = new(Queue)
		b.m[queueName] = queue
	}
	b.mu.Unlock()
	queue.Push(data)
}

func (b *Broker) getMsg(name string) string {
	b.mu.RLock()
	q, ok := b.m[name]
	b.mu.RUnlock()
	if !ok {
		return ""
	}
	return q.Pop()
}

func (b *Broker) GetMsg(name string, timeout time.Duration) string {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	data := b.getMsg(name)
LOOP:
	for data == "" {
		data = b.getMsg(name)
		select {
		case <-ctx.Done():
			break LOOP
		default:
		}
	}

	return data
}

func parseTimeout(t string) (time.Duration, error) {
	if t == "" {
		return 0, nil
	}
	seconds, err := strconv.Atoi(t)
	if err != nil || seconds < 0 {
		return 0, errors.New("invalid timeout value")
	}
	timeout := time.Second * time.Duration(seconds)
	return timeout, nil
}

func extractQueue(path string) string {
	return path[1:]
}

func (e *Endpoint) GetMsgHandler(w http.ResponseWriter, r *http.Request) {
	name := extractQueue(r.URL.Path)
	timeoutString := r.FormValue(timeoutParam)
	timeout, err := parseTimeout(timeoutString)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	if data := e.broker.GetMsg(name, timeout); data == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	} else {
		w.Write([]byte(data))
	}
}

func (e *Endpoint) PutMsgHandler(w http.ResponseWriter, r *http.Request) {
	msg := r.FormValue(msgParam)
	if msg == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	name := extractQueue(r.URL.Path)

	e.broker.PutMsg(name, msg)
	w.WriteHeader(http.StatusOK)
}

func (e *Endpoint) Route(w http.ResponseWriter, r *http.Request) {
	method := r.Method
	switch method {
	case "GET":
		e.GetMsgHandler(w, r)
	case "PUT":
		e.PutMsgHandler(w, r)
	default:
		http.NotFound(w, r)
	}
}

func getPort() (int, error) {
	if len(os.Args) == 2 {
		return strconv.Atoi(os.Args[1])
	}
	return 0, errors.New("port number was not provided")
}

func main() {
	broker := Broker{m: make(map[string]*Queue)}
	endpoint := &Endpoint{broker: broker}
	port, err := getPort()
	if err != nil {
		log.Fatal(err)
	}
	address := fmt.Sprintf(":%d", port)
	log.Printf("server starts on port = %d\n", port)
	http.HandleFunc("/", endpoint.Route)
	http.ListenAndServe(address, nil)
}
