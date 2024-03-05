package mapreduce

import (
	"sync"
)

func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var numOtherPhase int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)     // number of map tasks
		numOtherPhase = mr.nReduce // number of reducers
	case reducePhase:
		ntasks = mr.nReduce           // number of reduce tasks
		numOtherPhase = len(mr.files) // number of map tasks
	}
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, numOtherPhase)

	// WaitGroup to wait for all tasks to complete
	var wg sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		// Increment the WaitGroup counter
		wg.Add(1)

		go func(taskNumber int) {
			// Decrement the WaitGroup counter when the goroutine completes
			defer wg.Done()

			for {
				// Get a worker from the registerChannel and assign the task to it
				var workerAddress string = <-mr.registerChannel
				taskArgs := &RunTaskArgs{
					JobName:       mr.jobName,
					File:          mr.files[taskNumber],
					Phase:         phase,
					TaskNumber:    taskNumber,
					NumOtherPhase: numOtherPhase,
				}

				ok := call(workerAddress, "Worker.RunTask", taskArgs, new(struct{}))
				if ok {
					go func() {
						mr.registerChannel <- workerAddress
					}()
					break
				}
			}
		}(i) // Passing the task number to the goroutine
	}

	wg.Wait()

	debug("Schedule: %v phase done\n", phase)
}
