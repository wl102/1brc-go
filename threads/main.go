package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type chunk struct {
	Offset int64
	Size   int64
}

type City struct {
	Min   float64
	Max   float64
	Count int
	Total float64
}

func main() {
	// example.txt: chunk1/chunk2/chunk3/chunk4
	// 90m/3 = 30m
	name := os.Args[1]
	start_time := time.Now()
	// 把大文件分成cpu核心数量相等的区块，并行处理
	numChunk := runtime.NumCPU()
	// 每个区块对应offset和chunksize数字
	chunks := make([]chunk, numChunk)
	// 数据文件大小，bytes
	var dataLenght int64
	// 每个文件区块大小，bytes
	var sizeChunk int64
	var MaxBufferSize = 4 * 1024

	// 打开文件， 取命令行第二个参数为文件名
	file, err := os.Open(name)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	if fileInfo, err := file.Stat(); err != nil {
		log.Fatal(err)
	} else {
		dataLenght = fileInfo.Size()
	}

	sizeChunk = dataLenght / int64(numChunk)

	// 第0个区块的初始off
	prv := int64(0)
	// 第1个区块的初始off
	left := sizeChunk
	buf := make([]byte, MaxBufferSize)

	i := 1
	for ; i < numChunk; i++ {
		left = int64(i) * sizeChunk
	loop:
		for {
			n, err := file.ReadAt(buf, left)
			if err != nil {
				if err == io.EOF {
					break loop
				}
				log.Fatal(err)
			}
			for j := 0; j < n; j++ {
				if buf[j] == '\n' {
					left += int64(j)
					break loop
				}
			}
			left += int64(n)
		}
		chunks[i-1] = chunk{
			Offset: prv,
			Size:   left - prv,
		}
		prv = left
	}
	chunks[i-1] = chunk{
		Offset: prv,
		Size:   dataLenght - 1 - prv,
	}

	mps := make([]map[string]*City, numChunk)
	// 开始分区计算
	wg := sync.WaitGroup{}
	for i := 0; i < numChunk; i++ {
		mps[i] = make(map[string]*City)
		wg.Add(1)
		fmt.Println(chunks[i])
		go func(j int) {
			defer wg.Done()
			calculate(name, chunks[j], mps[j])
		}(i)
	}
	wg.Wait()

	// 合并
	results := make(map[string]*City)
	citys := make([]string, 0)
	for i := 0; i < numChunk; i++ {
		for cn, c := range mps[i] {
			if _, ok := results[cn]; ok {
				results[cn].Count += c.Count
				results[cn].Total += c.Total
				if results[cn].Max, ok = Max(results[cn].Max, c.Max); !ok {
					continue
				}
				results[cn].Min = Min(results[cn].Min, c.Min)
			} else {
				city := &City{
					Total: c.Total,
					Count: c.Count,
					Max:   c.Max,
					Min:   c.Min,
				}
				citys = append(citys, cn)
				results[cn] = city
			}
		}
	}
	//输出
	// 对输出的城市名按升序排列
	sort.Strings(citys)
	i = 0
	fmt.Print("{")
	for ; i < len(citys)-1; i++ {
		fmt.Printf("%s:%.1f/%.1f/%.1f, ", citys[i], results[citys[i]].Min,
			Avg(results[citys[i]].Total, results[citys[i]].Count), results[citys[i]].Max)
	}
	fmt.Printf("%s:%.1f/%.1f/%.1f", citys[i], results[citys[i]].Min,
		Avg(results[citys[i]].Total, results[citys[i]].Count), results[citys[i]].Max)
	fmt.Println("}")

	fmt.Printf("spend time:%.3fs\n", float64(time.Since(start_time).Milliseconds())/1000.0)
}

func calculate(name string, ck chunk, mp map[string]*City) {
	var total int64
	f, err := os.Open(name)
	if err != nil {
		log.Fatal(err)
	}
	off, err := f.Seek(ck.Offset, 0)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("read at", off)
	reader := bufio.NewReader(f)
	// 读取数据
	for {
		line, _, err := reader.ReadLine()
		if err != nil && err != io.EOF {
			log.Fatalf("read file err %s\n", err)
		}
		if err == io.EOF {
			break
		}
		total += int64(len(line) + 1)
		arr := strings.Split(string(line), ";")
		if len(arr) != 2 {
			continue
		}
		name := arr[0]
		temp, err := strconv.ParseFloat(arr[1], 32)
		if err != nil {
			log.Printf("parse float err:%s\n", err)
			continue
		}
		if city, ok := mp[name]; ok {
			city.Count += 1
			city.Total += temp
			if city.Max, ok = Max(city.Max, temp); !ok {
				continue
			}
			city.Min = Min(city.Min, temp)
		} else {
			city = &City{
				Total: temp,
				Count: 1,
				Max:   temp,
				Min:   temp,
			}
			mp[name] = city
		}
		if total >= ck.Size {
			break
		}
	}
}

func Max(x, y float64) (float64, bool) {
	if x > y {
		return x, true
	}
	return y, false
}

func Min(x, y float64) float64 {
	if x < y {
		return x
	}
	return y
}

func Avg(x float64, y int) float64 {
	return x / float64(y)
}

