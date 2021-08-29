package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

const (
	timeoutParam = "time"
	msgParam     = "v"
	pattern      = "/.*"
)

func init() {
	var err error
	reg, err = regexp.Compile(pattern)
	if err != nil {
		log.Fatal(err)
	}
}

var reg *regexp.Regexp

type Node struct {
	data string
	next *Node
}

type List struct {
	first *Node
	last  *Node
}

type Queue struct {
	mu   sync.Mutex
	list List
}

type Broker struct {
	m map[string]*Queue
}
type Endpoint struct {
	broker Broker
}

func (l *List) PushBack(node *Node) {
	if l.first == nil {
		l.first = node
		l.last = l.first
	} else {
		l.last.next = node
		l.last = node
	}
}

func (l *List) PopFront() *Node {
	node := l.first
	if l.first != nil {
		l.first = l.first.next
	}
	if l.first == nil {
		l.last = nil
	}
	return node
}

func (q *Queue) Push(msg string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	newNode := &Node{data: msg}
	q.list.PushBack(newNode)
}

func (q *Queue) Pop() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	node := q.list.PopFront()

	var data string
	if node != nil{
		data = node.data
	}
	return data
}

func (b *Broker) PutMsg(queueName, data string) {
	queue, ok := b.m[queueName]
	if !ok {
		queue = new(Queue)
		b.m[queueName] = queue
	}
	queue.Push(data)
}


func (b *Broker) GetMsg(name string) string{
	q, ok := b.m[name]
	if !ok{
		return ""
	}
	return q.Pop()
}

func (b *Broker) GetMsgWithTimeout(name string, timeout time.Duration) string{
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var data string

	for data == "" {
		select {
		case <-ctx.Done():
			return ""
		default:
			data = b.GetMsg(name)
		}
	}

	return data
}

func (e *Endpoint) GetMsgHandler(w http.ResponseWriter, r *http.Request) {
	name := extractQueueNameFromUrl(r.URL.Path)
	var data string
	if timeoutString := r.FormValue(timeoutParam); timeoutString != "" {
		seconds, err := strconv.Atoi(timeoutString)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		timeout := time.Second * time.Duration(seconds)
		data = e.broker.GetMsgWithTimeout(name, timeout)
	} else {
		data = e.broker.GetMsg(name)
	}

	if data == ""{
		http.NotFound(w, r)
		return
	}
	w.Write([]byte(data))
}

func (e *Endpoint) PutMsgHandler(w http.ResponseWriter, r *http.Request) {
	msg := r.FormValue(msgParam)
	if msg == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	name := extractQueueNameFromUrl(r.URL.Path)

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
func extractQueueNameFromUrl(path string) string {
	str := reg.FindString(path)
	if len(str) > 0{
		return str[1:]
	}
	return ""
}

func getPort() (int, error) {
	if len(os.Args) == 2 {
		return strconv.Atoi(os.Args[1])
	}
	return 0, errors.New("port number was not provided")
}

func main() {
	broker := Broker{make(map[string]*Queue)}
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
