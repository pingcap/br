package utils

type WorkerPool struct {
	limit   int
	workers chan *Worker
	name    string
}

type Worker struct {
	ID int64
}

func NewWorkerPool(limit int, name string) *WorkerPool {
	workers := make(chan *Worker, limit)
	for i := 0; i < limit; i++ {
		workers <- &Worker{ID: int64(i + 1)}
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