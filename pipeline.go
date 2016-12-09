package walrus

import (
	"runtime"
	"sync"
)

type PipelineFunc func(input jsMapFunctionInput, output chan<- interface{})

type Pipeline struct {
	funcs []PipelineFunc
	input <-chan jsMapFunctionInput
}

// Feeds the input channel through a number of copies of the function in parallel.
// This call is asynchronous. Output can be read from the returned channel.
func Parallelize(f PipelineFunc, parallelism int, input <-chan jsMapFunctionInput) <-chan interface{} {
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
