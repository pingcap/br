package utils

type WorkerPool struct {
	limit   uint
	workers chan *Worker
	name    string
}

type Worker struct {
	ID uint64
}

func NewWorkerPool(limit uint, name string) *WorkerPool {
	workers := make(chan *Worker, limit)
	for i := uint(0); i < limit; i++ {
		workers <- &Worker{ID: uint64(i + 1)}
	}
	return &WorkerPool{
		limit:   limit,
		workers: workers,
		name:    name,
	}
}

func (pool *WorkerPool) Apply() *Worker {
	worker := <-pool.workers
	return worker
}

func (pool *WorkerPool) Recycle(worker *Worker) {
	if worker == nil {
		panic("invalid restore worker")
	}
	pool.workers <- worker
}

func (pool *WorkerPool) HasWorker() bool {
	return len(pool.workers) > 0
}
