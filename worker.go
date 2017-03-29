package main

import (
	"fmt"
	"github.com/paulbellamy/ratecounter"
)

func NewWorker(id int, workerQueue chan chan *WorkRequest, counter *ratecounter.RateCounter) Worker {
	// Create, and return the worker.
	worker := Worker{
		Counter:     counter,
		ID:          id,
		Work:        make(chan *WorkRequest),
		WorkerQueue: workerQueue,
		QuitChan:    make(chan bool)}

	return worker
}

type Worker struct {
	Counter     *ratecounter.RateCounter
	ID          int
	Work        chan *WorkRequest
	WorkerQueue chan chan *WorkRequest
	QuitChan    chan bool
}

func (w *Worker) Start() {
	go func() {
		for {
			// Add ourselves into the worker queue.
			w.WorkerQueue <- w.Work

			select {
			case work := <-w.Work:
				w.Counter.Incr(1)
				if w.Counter.Rate() == 3333 {
					fmt.Println("Work: ", work)
				}
			case <-w.QuitChan:
				// We have been asked to stop.
				fmt.Printf("worker%d stopping\n", w.ID)
				return
			}
		}
	}()
}

func (w *Worker) Stop() {
	go func() {
		w.QuitChan <- true
	}()
}
