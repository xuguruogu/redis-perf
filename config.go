package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"

	"gopkg.in/yaml.v2"
)

var (
	// Conf ...
	Conf = &Config{}
	// RGen is Random generator ...
	RGen = &RandomGen{
		Seed: func() (seed []uint8) {
			var i uint8
			for i = 0; i < 10; i++ {
				seed = append(seed, 0)
			}
			for i = 'a'; i < 'z'; i++ {
				seed = append(seed, i)
			}
			for i = 'A'; i < 'Z'; i++ {
				seed = append(seed, i)
			}
			return seed
		}(),
	}
)

// Config ...
type Config struct {
	Addr      string
	DebugPort int
	QPS       int64
	Loop      int64
	Debug     bool
}

// Param ...
type Param struct {
	ValueLen      int64
	KeyNum        int64
	HashNum       int64
	HashSize      int64
	SetNum        int64
	SetSize       int64
	SortedSetNum  int64
	SortedSetSize int64
}

// RandomGen ...
type RandomGen struct {
	Param *Param
	Num   int64
	Range []*RangeParam
	Seed  []uint8
	Rand  []*rand.Rand
}

// RangeParam ...
type RangeParam struct {
	KeyMin        int64
	KeySize       int64
	HashMin       int64
	HashSize      int64
	SetMin        int64
	SetSize       int64
	SortedSetMin  int64
	SortedSetSize int64
}

// SortedSet gen random hash key ...
func (rg *RandomGen) SortedSet(id int) string {
	r := rg.Range[id]
	n := rg.Rand[id].Int63n(r.SortedSetSize) + r.SortedSetMin
	return fmt.Sprintf("sortedset_%012d_%012d_%012d", n, n, n)
}

// SortedSetField ...
func (rg *RandomGen) SortedSetField(id int) string {
	n := rg.Rand[id].Int63n(rg.Param.SortedSetSize)
	return fmt.Sprintf("sortedset_%012d_%012d_%012d", n, n, n)
}

// Set gen random hash key ...
func (rg *RandomGen) Set(id int) string {
	r := rg.Range[id]
	n := rg.Rand[id].Int63n(r.SetSize) + r.SetMin
	return fmt.Sprintf("set_%012d_%012d_%012d", n, n, n)
}

// SetField ...
func (rg *RandomGen) SetField(id int) string {
	n := rg.Rand[id].Int63n(rg.Param.SetSize)
	return fmt.Sprintf("set_%012d_%012d_%012d", n, n, n)
}

// Hash gen random hash key ...
func (rg *RandomGen) Hash(id int) string {
	r := rg.Range[id]
	n := rg.Rand[id].Int63n(r.HashSize) + r.HashMin
	return fmt.Sprintf("hash_%012d_%012d_%012d", n, n, n)
}

// HashField ...
func (rg *RandomGen) HashField(id int) string {
	n := rg.Rand[id].Int63n(rg.Param.HashSize)
	return fmt.Sprintf("hash_%012d_%012d_%012d", n, n, n)
}

// Key gen random normal key ...
func (rg *RandomGen) Key(id int) string {
	r := rg.Range[id]
	n := rg.Rand[id].Int63n(r.KeySize) + r.KeyMin
	return fmt.Sprintf("key_%012d_%012d_%012d", n, n, n)
}

// Value ...
func (rg *RandomGen) Value(id int) string {
	v := make([]uint8, rg.Param.ValueLen)
	for i := range v {
		v[i] = rg.Seed[rg.Rand[id].Intn(len(rg.Seed))]
	}
	return string(v)
}

// Score ...
func (rg *RandomGen) Score(id int) int {
	return rg.Rand[id].Int()%10 ^ 5
}

func init() {
	var configFile string
	var multiply int64
	flag.StringVar(&configFile, "p", "param.yml", "path to param config file")
	flag.StringVar(&Conf.Addr, "a", "127.0.0.1:6379", "redis server address")
	flag.IntVar(&Conf.DebugPort, "d", 7379, "perf debug address")
	flag.Int64Var(&Conf.QPS, "q", 10000, "desired qps")
	flag.Int64Var(&multiply, "m", 1, "multiply key number")
	flag.Int64Var(&RGen.Num, "n", 100, "concurrency number")
	flag.Int64Var(&Conf.Loop, "l", -1, "reconnect every l requests, l <= 0 means long connection")
	flag.BoolVar(&Conf.Debug, "debug", false, "debug")

	flag.Parse()
	if Conf.QPS <= 0 {
		log.Println("qps should not less than 0")
		os.Exit(0)
	}

	//random generator
	RGen.Param = LoadParam(configFile).Multiply(multiply)
	if RGen.Param.KeyNum < RGen.Num || RGen.Param.HashNum < RGen.Num || RGen.Param.SetNum < RGen.Num || RGen.Param.SortedSetNum < RGen.Num {
		log.Println("concurrency number should not less than KeyNum, HashNum, SetNum and SortedSetNum", RGen.Param.KeyNum, RGen.Param.HashNum, RGen.Param.SetNum, RGen.Param.SortedSetNum)
		os.Exit(0)
	}

	keys0 := RGen.Param.KeyNum / RGen.Num
	keys1 := keys0 + 1
	keyn0 := keys1*RGen.Num - RGen.Param.KeyNum
	// keyn1 := RGen.Param.KeyNum - keys0*RGen.Num

	hashs0 := RGen.Param.HashNum / RGen.Num
	hashs1 := hashs0 + 1
	hashn0 := hashs1*RGen.Num - RGen.Param.HashNum
	// hashn1 := RGen.Param.HashNum - hashs0*RGen.Num

	sets0 := RGen.Param.SetNum / RGen.Num
	sets1 := sets0 + 1
	setn0 := sets1*RGen.Num - RGen.Param.SetNum
	// setn1 := RGen.Param.SetNum - sets0*RGen.Num

	sortedsets0 := RGen.Param.SortedSetNum / RGen.Num
	sortedsets1 := sortedsets0 + 1
	sortedsetn0 := sortedsets1*RGen.Num - RGen.Param.SortedSetNum
	// sortedsetn1 := RGen.Param.SortedSetNum - sortedsets0*RGen.Num

	RGen.Range = make([]*RangeParam, RGen.Num)
	for i := range RGen.Range {
		r := &RangeParam{}
		//key
		if int64(i) < keyn0 {
			r.KeyMin = int64(i) * keys0
			r.KeySize = keys0
		} else {
			r.KeyMin = keyn0*keys0 + (int64(i)-keyn0)*keys1
			r.KeySize = keys1
		}
		//hash
		if int64(i) < hashn0 {
			r.HashMin = int64(i) * hashs0
			r.HashSize = hashs0
		} else {
			r.HashMin = hashn0*hashs0 + (int64(i)-hashn0)*hashs1
			r.HashSize = hashs1
		}
		//set
		if int64(i) < setn0 {
			r.SetMin = int64(i) * sets0
			r.SetSize = sets0
		} else {
			r.SetMin = setn0*sets0 + (int64(i)-setn0)*sets1
			r.SetSize = sets1
		}
		//sortedset
		if int64(i) < sortedsetn0 {
			r.SortedSetMin = int64(i) * sortedsets0
			r.SortedSetSize = sortedsets0
		} else {
			r.SortedSetMin = sortedsetn0*sortedsets0 + (int64(i)-sortedsetn0)*sortedsets1
			r.SortedSetSize = sortedsets1
		}

		RGen.Range[i] = r
	}
	RGen.Rand = make([]*rand.Rand, RGen.Num)
	for i := range RGen.Rand {
		RGen.Rand[i] = rand.New(rand.NewSource(int64(i)))
	}

	if Conf.Debug {
		for _, r := range RGen.Range {
			log.Println(r)
		}
	}
}

// LoadParam ...
func LoadParam(configFile string) (param *Param) {
	var content []byte
	content, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Println("read config file error, use default config")
		return (&Param{}).Default()
	}

	param = &Param{}
	if err := yaml.Unmarshal(content, param); err != nil {
		log.Println("unmarshal config file error, use default config")
		return (&Param{}).Default()
	}

	return param.Default()
}

// Default ...
func (param *Param) Default() *Param {
	if param.ValueLen == 0 {
		param.ValueLen = 100
	}
	if param.KeyNum == 0 {
		param.KeyNum = 10000000
	}
	if param.HashNum == 0 {
		param.HashNum = 5000
	}
	if param.HashSize == 0 {
		param.HashSize = 50
	}
	if param.SetNum == 0 {
		param.SetNum = 5000
	}
	if param.SetSize == 0 {
		param.SetSize = 50
	}
	if param.SortedSetNum == 0 {
		param.SortedSetNum = 5000
	}
	if param.SortedSetSize == 0 {
		param.SortedSetSize = 50
	}
	return param
}

// Multiply ...
func (param *Param) Multiply(multiply int64) *Param {
	param.KeyNum *= multiply
	param.HashNum *= multiply
	param.SetNum *= multiply
	param.SortedSetNum *= multiply

	return param
}
