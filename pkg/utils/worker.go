package utils

// WorkerPool contains a pool of workers
type WorkerPool struct {
	limit   uint
	workers chan *Worker
	name    string
}

// Worker identified by ID
type Worker struct {
	ID uint64
}

// NewWorkerPool returns a WorkPool
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

// Apply acquires a worker
func (pool *WorkerPool) Apply() *Worker {
	worker := <-pool.workers
	return worker
}

// Recycle releases a worker
func (pool *WorkerPool) Recycle(worker *Worker) {
	if worker == nil {
		panic("invalid restore worker")
	}
	pool.workers <- worker
}

// HasWorker checks if the pool has unallocated workers
func (pool *WorkerPool) HasWorker() bool {
	return len(pool.workers) > 0
}
