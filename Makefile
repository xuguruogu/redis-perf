export GOPATH=$(CURDIR)/.build
export GO15VENDOREXPERIMENT=1

all: compile

compile: redis-perf

redis-perf:
	go build -o bin/redis-perf github.com/xuguruogu/redis-perf