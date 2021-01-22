package cloudgateproxy

import "sync"

type Scheduler struct {
	queue chan func()
	wg    *sync.WaitGroup
}

func NewScheduler(workers int) *Scheduler {
	scheduler := &Scheduler{
		queue: make(chan func(), workers),
		wg:    &sync.WaitGroup{},
	}

	for i := 0; i < workers; i++ {
		scheduler.wg.Add(1)
		go func() {
			defer scheduler.wg.Done()
			for {
				task, ok := <- scheduler.queue
				if !ok {
					return
				}
				task()
			}
		}()
	}

	return scheduler
}

func (recv *Scheduler) Schedule(task func()) {
	recv.queue <- task
}

func (recv *Scheduler) Shutdown() {
	close(recv.queue)
	recv.wg.Wait()
}