package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type City struct {
	Name  string
	Min   float64
	Max   float64
	Count int
	Total float64
}

func main() {
	start_time := time.Now()
	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(f)
	// 保存数据的map, cityName -> temperature
	data := make(map[string]*City)
	citys := make([]string, 0)

	// 读取数据
	for {
		line, _, err := reader.ReadLine()
		if err != nil && err != io.EOF {
			log.Fatalf("read file err %s\n", err)
		}
		if err == io.EOF {
			break
		}
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
		if city, ok := data[name]; ok {
			city.Count += 1
			city.Total += temp
			if city.Max, ok = Max(city.Max, temp); !ok {
				continue
			}
			city.Min = Min(city.Min, temp)
		} else {
			city = &City{
				Name:  name,
				Total: temp,
				Count: 1,
				Max:   temp,
				Min:   temp,
			}
			citys = append(citys, name)
			data[name] = city
		}
	}

	fmt.Println(int64(10) / int64(3))
	// 输出结果
	// 对输出的城市名按升序排列
	sort.Strings(citys)
	i := 0
	fmt.Print("{")
	for ; i < len(citys)-1; i++ {
		fmt.Printf("%s:%.1f/%.1f/%.1f, ", citys[i], data[citys[i]].Min,
			Avg(data[citys[i]].Total, data[citys[i]].Count), data[citys[i]].Max)
	}
	fmt.Printf("%s:%.1f/%.1f/%.1f", citys[i], data[citys[i]].Min,
		Avg(data[citys[i]].Total, data[citys[i]].Count), data[citys[i]].Max)
	fmt.Println("}")

	fmt.Printf("spend time:%.3fs\n", float64(time.Since(start_time).Milliseconds())/1000.0)
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

