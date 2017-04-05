package main

import (
	"log"
	"os"
	"time"
	"unsafe"

	"sync/atomic"

	"github.com/garyburd/redigo/redis"
)

// Result ...
type Result struct {
	QPS   int64
	Num   int64
	Delay int64
	Err   int64
}

// BucketStatus ...
type BucketStatus struct {
	Num   int64
	Delay int64
	Err   int64
}

// Perf ...
type Perf struct {
	addr          string
	loop          int64
	qps           int64
	connTotalNum  int64
	needReconnect int64
}

// GetConn ...
func (p *Perf) GetConn() redis.Conn {
	reconnect := false
	for {
		conn, err := redis.Dial("tcp", p.addr, redis.DialConnectTimeout(time.Second))
		if err != nil {
			if reconnect == false {
				reconnect = true
				atomic.AddInt64(&p.needReconnect, 1)
			}
			if atomic.LoadInt64(&p.needReconnect) == p.connTotalNum {
				log.Println(err)
				os.Exit(1)
			}
			time.Sleep(time.Millisecond * 10)
			continue
		}
		if reconnect {
			atomic.AddInt64(&p.needReconnect, -1)
		}
		return conn
	}
}

// TokenBucketWorker ...
type TokenBucketWorker struct {
	id           int
	token        chan int64
	bucketStatus unsafe.Pointer
	perf         *Perf
}

// NewTokenBucketWorker ...
func NewTokenBucketWorker(id int, perf *Perf) (w *TokenBucketWorker) {
	w = &TokenBucketWorker{
		id:           id,
		token:        make(chan int64, 100),
		perf:         perf,
		bucketStatus: unsafe.Pointer(&BucketStatus{}),
	}
	tasks := w.LoopWriter()
	w.LoopReader(tasks)
	return w
}

// GetAndResetBucketStatus ...
func (w *TokenBucketWorker) GetAndResetBucketStatus() (status *BucketStatus) {
	return (*BucketStatus)(atomic.SwapPointer(&w.bucketStatus, unsafe.Pointer(&BucketStatus{})))
}

// GetBucketStatus ...
func (w *TokenBucketWorker) GetBucketStatus() (status *BucketStatus) {
	return (*BucketStatus)(atomic.LoadPointer(&w.bucketStatus))
}

// LoopWriter ...
func (w *TokenBucketWorker) LoopWriter() (tasks chan *Request) {
	tasks = make(chan *Request, 100000)
	go func() {
		var conn redis.Conn
		var integral int64
		loop := w.perf.loop
		id := w.id

		for n := range w.token {
			integral += n
			for integral > 0 {
				if conn == nil || conn.Err() != nil {
					conn = w.perf.GetConn()
					loop = w.perf.loop
				}

				rs := AllExecutor.Execute(conn, id)
				integral -= int64(len(rs))
				for _, r := range rs {
					r.Conn = conn
					r.RecordStart()
				}

				if w.perf.loop > 0 {
					loop -= int64(len(rs))
					if loop <= 0 {
						conn = nil
						rs[len(rs)-1].Last = true
					}
				}

				for _, r := range rs {
					tasks <- r
				}
			}
		}
	}()

	return tasks
}

// LoopReader ...
func (w *TokenBucketWorker) LoopReader(tasks chan *Request) {
	go func() {
		for r := range tasks {
			reply, err := r.Conn.Receive()
			r.Err = r.valid(reply, err)
			r.RecordStop()
			if r.Last {
				r.Conn.Close()
			}

			status := w.GetBucketStatus()
			status.Num++
			status.Delay += r.ResponseTime()
			if r.Err != nil {
				if Conf.Debug {
					log.Println(r)
				}
				status.Err++
			}
		}
	}()
}

// NewPerfGen ...
func NewPerfGen(addr string, qps, num, loop int64) (result chan *Result) {
	perf := &Perf{
		addr:         addr,
		loop:         loop,
		qps:          qps,
		connTotalNum: num,
	}
	result = make(chan *Result, 100)
	workers := make([]*TokenBucketWorker, num)
	for index := range workers {
		workers[index] = NewTokenBucketWorker(index, perf)
	}

	go BucketGenToken(workers, perf)
	return GenResult(workers)
}

// BucketGenToken ...
func BucketGenToken(workers []*TokenBucketWorker, perf *Perf) {
	t := time.NewTicker(time.Millisecond)
	clock := 0
	qps := perf.qps
	num := perf.connTotalNum

	for {
		select {
		case <-t.C:
			allocation := qps / 1000
			s0 := allocation / num
			s1 := s0 + 1
			n0 := s1*num - allocation
			n1 := allocation - s0*num

			for i := 0; i < int(n1); i++ {
				select {
				case workers[clock].token <- s1:
				}
				clock = (clock + 1) % int(num)
			}
			if s0 > 0 {
				for i := 0; i < int(n0); i++ {
					select {
					case workers[(clock+i)%int(num)].token <- s0:
					}
				}
			}
		}
	}

}

// GenResult ...
func GenResult(workers []*TokenBucketWorker) (result chan *Result) {
	result = make(chan *Result, 100)

	t := time.NewTicker(time.Second)
	go func() {
		for range t.C {

			sl := make([]*BucketStatus, len(workers))
			for index, worker := range workers {
				sl[index] = worker.GetAndResetBucketStatus()
			}

			r := &Result{}
			for _, s := range sl {
				r.Delay += s.Delay
				r.Err += s.Err
				r.Num += s.Num
			}

			r.QPS = r.Num
			if r.Num > 0 {
				r.Delay = r.Delay / r.Num
			}

			select {
			case result <- r:
			}
		}
	}()

	return result
}
