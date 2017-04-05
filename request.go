package main

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

// Request ...
type Request struct {
	Opstr string
	valid func(reply interface{}, err error) error
	Err   error
	Start int64
	Stop  int64
	Last  bool
	Conn  redis.Conn
}

// RecordStart ...
func (r *Request) RecordStart() {
	r.Start = time.Now().UnixNano() / 1000
}

// RecordStop ...
func (r *Request) RecordStop() {
	r.Stop = time.Now().UnixNano() / 1000
}

// ResponseTime ...
func (r *Request) ResponseTime() int64 {
	return r.Stop - r.Start
}
