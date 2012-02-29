package gostalker

import (
  "container/heap"
  "sync"
)

type Tube struct {
  name          string

  ready         *readyJobs
  readyMutex    *sync.Mutex

  reserved      *reservedJobs
  reservedMutex *sync.Mutex

  buried        *buriedJobs
  buriedMutex   *sync.Mutex

  delayed       *delayedJobs
  delayedMutex  *sync.Mutex
}

func newTube(name string) (tube *Tube) {
  tube = &Tube{
    name:          name,

    ready:         &readyJobs{},
    readyMutex:    &sync.Mutex{},

    reserved:      &reservedJobs{},
    reservedMutex: &sync.Mutex{},

    buried:        newBuriedJobs(),
    buriedMutex:   &sync.Mutex{},

    delayed:       &delayedJobs{},
    delayedMutex:  &sync.Mutex{},
  }

  heap.Init(tube.ready)
  heap.Init(tube.reserved)
  heap.Init(tube.delayed)

  return
}

func heapPush(mutex *sync.Mutex, arr heap.Interface, job *Job) {
  mutex.Lock()
  heap.Push(arr, job)
  mutex.Unlock()
}

func heapPop(mutex *sync.Mutex, arr heap.Interface) (job *Job) {
  mutex.Lock()
  job = heap.Pop(arr).(*Job)
  mutex.Unlock()
  return
}

func (tube *Tube) readyPut(job *Job) {
  heapPush(tube.readyMutex, tube.ready, job)
}

func (tube *Tube) readyReserve() (job *Job) {
  return heapPop(tube.readyMutex, tube.ready)
}

func (tube *Tube) delayedPut(job *Job) {
  heapPush(tube.delayedMutex, tube.delayed, job)
}
func (tube *Tube) reservedRelease(id JobId) {}
func (tube *Tube) reservedDelete(id JobId)  {}
func (tube *Tube) reservedBury(id JobId)    {}
func (tube *Tube) buriedDelete(id JobId)    {}
func (tube *Tube) buriedKick(id JobId)      {}
