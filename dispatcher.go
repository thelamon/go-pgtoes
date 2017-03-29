package main

import (
	"fmt"
	"github.com/paulbellamy/ratecounter"
	"time"
)

var WorkerQueue chan chan *WorkRequest

func StartDispatcher(nworkers int) {
	// First, initialize the channel we are going to but the workers' work channels into.
	WorkerQueue = make(chan chan *WorkRequest, nworkers)

	c := ratecounter.NewRateCounter(1 * time.Second)

	// Now, create all of our workers.
	for i := 0; i < nworkers; i++ {
		fmt.Println("Starting worker", i+1)
		worker := NewWorker(i+1, WorkerQueue, c)
		worker.Start()
	}

	go func() {
		for {
			select {
			case work := <-WorkQueue:
				//if work.id == 333333 {
				//	fmt.Println(work)
				//}
				go func() {
					worker := <-WorkerQueue
					worker <- work
				}()
			}
		}
	}()
}
