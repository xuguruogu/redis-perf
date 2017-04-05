package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("ctrl-c or SIGTERM found, exit")
		os.Exit(0)
	}()

	go func() {
		fmt.Println(http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", Conf.DebugPort), nil))
	}()

	addr := Conf.Addr
	qps := Conf.QPS
	num := RGen.Num
	loop := Conf.Loop

	result := NewPerfGen(addr, qps, num, loop)
	for r := range result {
		log.Printf("expect %d\tqps %d\tdelay %dus\terr %d\n", qps, r.QPS, r.Delay, r.Err)
	}
}
