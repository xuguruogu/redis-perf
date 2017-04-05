package main

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
)

var (
	AllExecutor       = &RandomExecutor{}
	KeyExecutor       = &RandomExecutor{}
	HashExecutor      = &RandomExecutor{}
	SetExecutor       = &RandomExecutor{}
	SortedSetExecutor = &RandomExecutor{}
)

func init() {
	// AllExecutor
	AllExecutor.Add(5, KeyExecutor.Execute)
	AllExecutor.Add(2, HashExecutor.Execute)
	AllExecutor.Add(1, SetExecutor.Execute)
	AllExecutor.Add(1, SortedSetExecutor.Execute)
	// KeyExecutor
	// set and get
	KeyExecutor.Add(10, func(conn redis.Conn, id int) (rs []*Request) {
		key := RGen.Key(id)
		value := RGen.Value(id)

		// set
		conn.Send("SET", key, value)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("SET %s %s", key, value),
			valid: func(reply interface{}, err error) error {
				result, err := redis.String(reply, err)
				if err != nil {
					return err
				}
				if result != "OK" {
					return fmt.Errorf("expect OK, get %s", result)
				}
				return nil
			},
		})
		conn.Flush()

		// get
		conn.Send("GET", key)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("GET %s", key),
			valid: func(reply interface{}, err error) error {
				result, err := redis.String(reply, err)
				if err != nil {
					return err
				}
				if result != value {
					return fmt.Errorf("expect %s, get %s", value, result)
				}
				return nil
			},
		})
		conn.Flush()

		return rs
	})

	// set exist del
	KeyExecutor.Add(3, func(conn redis.Conn, id int) (rs []*Request) {
		key := RGen.Key(id)
		value := RGen.Value(id)

		// set
		conn.Send("SET", key, value)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("SET %s %s", key, value),
			valid: func(reply interface{}, err error) error {
				result, err := redis.String(reply, err)
				if err != nil {
					return err
				}
				if result != "OK" {
					return fmt.Errorf("expect OK, get %s", result)
				}
				return nil
			},
		})
		conn.Flush()

		conn.Send("EXISTS", key)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("EXISTS %s", key),
			valid: func(reply interface{}, err error) error {
				result, err := redis.Int(reply, err)
				if err != nil {
					return err
				}
				if result != 1 {
					return fmt.Errorf("expect 1, get %d", result)
				}
				return nil
			},
		})
		conn.Flush()

		conn.Send("DEL", key)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("DEL %s", key),
			valid: func(reply interface{}, err error) error {
				result, err := redis.Int(reply, err)
				if err != nil {
					return err
				}
				if result != 1 {
					return fmt.Errorf("expect 1, get %d", result)
				}
				return nil
			},
		})
		conn.Flush()

		return rs
	})

	// HashExecutor
	// hset and hget
	HashExecutor.Add(10, func(conn redis.Conn, id int) (rs []*Request) {
		key := RGen.Hash(id)
		field := RGen.HashField(id)
		value := RGen.Value(id)

		conn.Send("HSET", key, field, value)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("HSET %s %s %s", key, field, value),
			valid: func(reply interface{}, err error) error {
				result, err := redis.Int(reply, err)
				if err != nil {
					return err
				}
				if result != 0 && result != 1 {
					return fmt.Errorf("expect 0 or 1, get %d", result)
				}
				return nil
			},
		})
		conn.Flush()

		conn.Send("HGET", key, field)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("HGET %s %s", key, field),
			valid: func(reply interface{}, err error) error {
				result, err := redis.String(reply, err)
				if err != nil {
					return err
				}
				if result != value {
					return fmt.Errorf("expect %s, get %s", value, result)
				}
				return nil
			},
		})
		conn.Flush()

		return rs
	})

	// hset and hdel
	HashExecutor.Add(3, func(conn redis.Conn, id int) (rs []*Request) {
		key := RGen.Hash(id)
		field := RGen.HashField(id)
		value := RGen.Value(id)

		conn.Send("HSET", key, field, value)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("HSET %s %s %s", key, field, value),
			valid: func(reply interface{}, err error) error {
				result, err := redis.Int(reply, err)
				if err != nil {
					return err
				}
				if result != 0 && result != 1 {
					return fmt.Errorf("expect 0 or 1, get %d", result)
				}
				return nil
			},
		})
		conn.Flush()

		conn.Send("HDEL", key, field)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("HDEL %s %s", key, field),
			valid: func(reply interface{}, err error) error {
				result, err := redis.Int(reply, err)
				if err != nil {
					return err
				}
				if result != 1 {
					return fmt.Errorf("expect 1, get %d", result)
				}
				return nil
			},
		})
		conn.Flush()

		return rs
	})

	// HLEN and HGETALL
	HashExecutor.Add(1, func(conn redis.Conn, id int) (rs []*Request) {
		key := RGen.Hash(id)
		var length int

		conn.Send("HLEN", key)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("HLEN %s", key),
			valid: func(reply interface{}, err error) error {
				result, err := redis.Int(reply, err)
				if err != nil {
					return err
				}
				length = result
				return nil
			},
		})
		conn.Flush()

		conn.Send("HGETALL", key)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("HGETALL %s", key),
			valid: func(reply interface{}, err error) error {
				result, err := redis.StringMap(reply, err)
				if err != nil {
					return err
				}
				if len(result) != length {
					return fmt.Errorf("expect length %d, get %d", length, len(result))
				}
				return nil
			},
		})
		conn.Flush()

		return rs
	})

	// SetExecutor
	// SADD and SCARD
	SetExecutor.Add(10, func(conn redis.Conn, id int) (rs []*Request) {
		key := RGen.Set(id)
		field := RGen.SetField(id)

		conn.Send("SADD", key, field)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("SADD %s %s", key, field),
			valid: func(reply interface{}, err error) error {
				result, err := redis.Int(reply, err)
				if err != nil {
					return err
				}
				if result != 0 && result != 1 {
					return fmt.Errorf("expect 0 or 1, get %d", result)
				}
				return nil
			},
		})
		conn.Flush()

		conn.Send("SCARD", key)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("SCARD %s", key),
			valid: func(reply interface{}, err error) error {
				result, err := redis.Int(reply, err)
				if err != nil {
					return err
				}
				if result <= 0 {
					return fmt.Errorf("expect larger than 0, get %s", result)
				}
				return nil
			},
		})
		conn.Flush()

		return rs
	})

	// SCARD and SMEMBERS
	SetExecutor.Add(1, func(conn redis.Conn, id int) (rs []*Request) {
		key := RGen.Set(id)
		var length int

		conn.Send("SCARD", key)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("SCARD %s", key),
			valid: func(reply interface{}, err error) error {
				result, err := redis.Int(reply, err)
				if err != nil {
					return err
				}
				length = result
				return nil
			},
		})
		conn.Flush()

		conn.Send("SMEMBERS", key)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("SMEMBERS %s", key),
			valid: func(reply interface{}, err error) error {
				result, err := redis.Strings(reply, err)
				if err != nil {
					return err
				}
				if len(result) != length {
					return fmt.Errorf("expect length %d, get %d", length, len(result))
				}
				return nil
			},
		})
		conn.Flush()

		return rs
	})

	// SortedSetExecutor
	// ZADD and ZCARD
	SortedSetExecutor.Add(10, func(conn redis.Conn, id int) (rs []*Request) {
		key := RGen.SortedSet(id)
		score := RGen.Score(id)
		field := RGen.SortedSetField(id)

		conn.Send("ZADD", key, score, field)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("SADD %s %d %s", key, score, field),
			valid: func(reply interface{}, err error) error {
				result, err := redis.Int(reply, err)
				if err != nil {
					return err
				}
				if result != 0 && result != 1 {
					return fmt.Errorf("expect 0 or 1, get %d", result)
				}
				return nil
			},
		})
		conn.Flush()

		conn.Send("ZCARD", key)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("SCARD %s", key),
			valid: func(reply interface{}, err error) error {
				result, err := redis.Int(reply, err)
				if err != nil {
					return err
				}
				if result <= 0 {
					return fmt.Errorf("expect larger than 0, get %s", result)
				}
				return nil
			},
		})
		conn.Flush()

		return rs
	})

	// ZCOUNT and ZRANGEBYSCORE
	SortedSetExecutor.Add(1, func(conn redis.Conn, id int) (rs []*Request) {
		key := RGen.SortedSet(id)
		min, max := RGen.Score(id), RGen.Score(id)
		if min > max {
			min, max = max, min
		}
		var length int

		conn.Send("ZCOUNT", key, min, max)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("ZCOUNT %s %d %d", key, min, max),
			valid: func(reply interface{}, err error) error {
				result, err := redis.Int(reply, err)
				if err != nil {
					return err
				}
				length = result
				return nil
			},
		})
		conn.Flush()

		conn.Send("ZRANGEBYSCORE", key, min, max)
		rs = append(rs, &Request{
			Opstr: fmt.Sprintf("ZRANGEBYSCORE %s %d %d", key, min, max),
			valid: func(reply interface{}, err error) error {
				result, err := redis.Strings(reply, err)
				if err != nil {
					return err
				}
				if len(result) != length {
					return fmt.Errorf("expect length %d, get %s", length, result)
				}
				return nil
			},
		})
		conn.Flush()
		return rs
	})

}

// RandomExecutor ...
type RandomExecutor struct {
	items []func(conn redis.Conn, id int) (rs []*Request)
}

// Execute ...
func (re *RandomExecutor) Execute(conn redis.Conn, id int) (rs []*Request) {
	return re.items[RGen.Rand[id].Int()%len(re.items)](conn, id)
}

// Add ...
func (re *RandomExecutor) Add(score int64, execute func(conn redis.Conn, id int) (rs []*Request)) {
	tmp := make([]func(conn redis.Conn, id int) (rs []*Request), score)
	for i := range tmp {
		tmp[i] = execute
	}
	re.items = append(re.items, tmp...)
}
