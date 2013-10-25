package walrus

import (
	"runtime"
	"sync"
)

type PipelineFunc func(input interface{}, output chan<- interface{})

type Pipeline struct {
	funcs []PipelineFunc
	input <-chan interface{}
}

func NewPipeline(chanSize int, parallelism int, funcs ...PipelineFunc) {
	p := Pipeline{
		funcs: funcs,
		input: make(chan interface{}, chanSize),
	}
	var input <-chan interface{} = p.input
	for _, f := range funcs {
		input = Parallelize(f, parallelism, input)
	}
}

// Feeds the input channel through a number of copies of the function in parallel.
// This call is asynchronous. Output can be read from the returned channel.
func Parallelize(f PipelineFunc, parallelism int, input <-chan interface{}) <-chan interface{} {
	if parallelism == 0 {
		parallelism = runtime.GOMAXPROCS(0)
	}
	output := make(chan interface{}, len(input))
	var waiter sync.WaitGroup
	for j := 0; j < parallelism; j++ {
		waiter.Add(1)
		go func() {
			defer waiter.Done()
			for item := range input {
				f(item, output)
			}
		}()
	}
	go func() {
		waiter.Wait()
		close(output)
	}()
	return output
}
