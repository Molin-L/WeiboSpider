package wbspider

import "net/http"

type Gateway struct {
	requestChan chan *http.Request
	retryChan   chan *http.Request
}

func NewGateway() *Gateway {
	return &Gateway{
		requestChan: make(chan *http.Request),
		retryChan:   make(chan *http.Request),
	}
}

func (gw *Gateway) Start() {
	go gw.start()
}

func (gw *Gateway) start() {
	for {
		select {
		case req := <-gw.requestChan:
			gw.handleRequest(req)
		case req := <-gw.retryChan:
			gw.handleRetry(req)
		}
	}
}

func (gw *Gateway) handleRequest(req *http.Request) {
}

func (gw *Gateway) handleRetry(req *http.Request) {

}
