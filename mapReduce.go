package mapreduce

type (
	flow[V any]     chan V
	readFlow[V any] <-chan V
)

func NewFlow[V any](v []V) readFlow[V] {
	out := make(chan V)
	go func() {
		defer close(out)
		for _, v := range v {
			out <- v
		}
	}()
	return out
}

func ForEach[I any](in readFlow[I], fn func(I)) {
	for v := range in {
		fn(v)
	}
}

func Map[I any, O any](in readFlow[I], fn func(I) O) readFlow[O] {
	out := make(chan O)
	go func() {
		defer close(out)
		for i := range in {
			out <- fn(i)
		}
	}()
	return out
}

func Filter[I any](in readFlow[I], fn func(I) bool) readFlow[I] {
	out := make(chan I)
	go func() {
		defer close(out)
		for i := range in {
			if fn(i) {
				out <- i
			}
		}
	}()
	return out
}

func Reduce[I any](in readFlow[I], fn func(I, I) I) I {
	res := <-in
	for i := range in {
		res = fn(res, i)
	}
	return res
}
