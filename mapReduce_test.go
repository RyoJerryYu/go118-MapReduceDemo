package mapreduce_test

import (
	"testing"

	mapreduce "github.com/RyoJerryYu/go118-MapReduceDemo"
	"github.com/stretchr/testify/assert"
)

type testCase []int

func initTestCases() []testCase {
	bigCase := make([]int, 0, 100)
	for i := 0; i < 100; i++ {
		bigCase = append(bigCase, i)
	}

	cases := []testCase{
		{1, 2, 3},
		{},
		bigCase,
	}
	return cases
}

func TestNewFlow(t *testing.T) {
	cases := initTestCases()

	for _, tc := range cases {
		dict := make(map[int]bool)
		for _, v := range tc {
			dict[v] = false
		}
		flow := mapreduce.NewFlow(tc)
		for v := range flow {
			dict[v] = true
		}

		for _, v := range dict {
			assert.True(t, v)
		}
	}
}

func TestForEach(t *testing.T) {
	cases := initTestCases()

	for _, tc := range cases {
		dict := make(map[int]bool)
		for _, v := range tc {
			dict[v] = false
		}
		flow := mapreduce.NewFlow(tc)

		mapreduce.ForEach(flow, func(v int) {
			dict[v] = true
		})

		for _, v := range dict {
			assert.True(t, v)
		}
	}
}

func TestMap(t *testing.T) {
	cases := initTestCases()

	for _, tc := range cases {
		res := make([]int, 0, len(tc))
		for _, v := range tc {
			res = append(res, v*v*v*v)
		}

		f1 := mapreduce.NewFlow(tc)
		f2 := mapreduce.Map(f1, func(v int) int {
			return v * v
		})
		f3 := mapreduce.Map(f2, func(v int) int {
			return v * v
		})

		i := 0
		for out := range f3 {
			assert.Equal(t, res[i], out)
			i++
		}
	}
}

func TestFilter(t *testing.T) {
	cases := initTestCases()

	for _, tc := range cases {
		res := make([]int, 0)
		for _, v := range tc {
			if v%2 == 0 && v%3 == 0 {
				res = append(res, v)
			}
		}

		f1 := mapreduce.NewFlow(tc)
		f2 := mapreduce.Filter(f1, func(v int) bool {
			return v%2 == 0
		})
		f3 := mapreduce.Filter(f2, func(v int) bool {
			return v%3 == 0
		})

		i := 0
		for out := range f3 {
			assert.Equal(t, res[i], out)
			i++
		}
	}
}

func TestReduce(t *testing.T) {
	cases := initTestCases()

	for _, tc := range cases {
		res := 0
		for _, v := range tc {
			res = res*2 + v
		}

		f1 := mapreduce.NewFlow(tc)
		out := mapreduce.Reduce(f1, func(v1, v2 int) int {
			return v1*2 + v2
		})

		assert.Equal(t, res, out)
	}
}

func TestAll(t *testing.T) {
	data := make([]int, 0, 100)
	for i := 0; i < 100; i++ {
		data = append(data, i)
	}

	res := 0
	for i := 0; i < 10; i++ {
		res += i*10 + 2
	}

	f1 := mapreduce.NewFlow(data)
	f2 := mapreduce.Filter(f1, func(v int) bool {
		return v%10 == 0
	})
	f3 := mapreduce.Map(f2, func(v int) int {
		return v + 2
	})
	out := mapreduce.Reduce(f3, func(v1, v2 int) int {
		return v1 + v2
	})

	assert.Equal(t, res, out)
}
