package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"os"
	"sort"
)

func runMapTask(
	jobName string, // The name of the whole mapreduce job
	mapTaskIndex int, // The index of the map task
	inputFile string, // The path to the input file assigned to this task
	nReduce int, // The number of reduce tasks that will be run
	mapFn func(file string, contents string) []KeyValue, // The user-defined map function
) {
	// Reading the contents of the inputFile
	contents, err := os.ReadFile(inputFile)
	if err != nil {
		panic(err)
	}
	fileContents := string(contents)

	// Calling the user-defined map function
	keyValuePairs := mapFn(inputFile, fileContents)

	// Creating intermediate files for reduce tasks
	for reduceTaskIndex := 0; reduceTaskIndex < nReduce; reduceTaskIndex++ {

		intermediateFileName := getIntermediateName(jobName, mapTaskIndex, reduceTaskIndex)

		intermediateFile, err := os.Create(intermediateFileName)
		if err != nil {
			panic(err)
		}
		defer intermediateFile.Close()
	}

	// Writing the key-value pairs to the intermediate files
	for _, kv := range keyValuePairs {

		hashValue := hash32(kv.Key) // kv.Key is already a string since KeyValue is a struct of {string, string}
		reduceIndex := int(hashValue) % nReduce

		intermediateFileName := getIntermediateName(jobName, mapTaskIndex, reduceIndex)
		intermediateFile, err := os.OpenFile(intermediateFileName, os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}

		encoder := json.NewEncoder(intermediateFile)
		err = encoder.Encode(&kv)
		if err != nil {
			panic(err)
		}
		defer intermediateFile.Close()
	}

}

func hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func runReduceTask(
	jobName string, // the name of the whole MapReduce job
	reduceTaskIndex int, // the index of the reduce task
	nMap int, // the number of map tasks that were run
	reduceFn func(key string, values []string) string,
) {

	// Map in which the key-value pairs will be grouped
	intermediateKeyValuePairs := make(map[string][]string)

	// Loop over map tasks and loading corresponding intermediate files
	for mapTaskIndex := 0; mapTaskIndex < nMap; mapTaskIndex++ {
		intermediateFileName := getIntermediateName(jobName, mapTaskIndex, reduceTaskIndex)
		intermediateFile, err := os.OpenFile(intermediateFileName, os.O_RDONLY, 0666)
		if err != nil {
			panic(err)
		}
		defer intermediateFile.Close()

		// Reading intermediate files and grouping the received key-value pairs by key
		decoder := json.NewDecoder(intermediateFile)
		for {
			var kv KeyValue
			isDone := decoder.Decode(&kv)
			if isDone != nil {
				break
			}
			intermediateKeyValuePairs[kv.Key] = append(intermediateKeyValuePairs[kv.Key], kv.Value)
		}

	}

	// Sorting the keys (required in the assignment)
	var sortedKeys []string
	for key := range intermediateKeyValuePairs {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	// Preparing the output file
	outputFileName := getReduceOutName(jobName, reduceTaskIndex)
	outputFile, err := os.OpenFile(outputFileName, os.O_APPEND|os.O_CREATE, 0666) // os.O_CREATE is used instead of a separate os.Create() call
	if err != nil {
		panic(err)
	}
	defer outputFile.Close()

	// Processing the key-value pairs in sorted order
	encoder := json.NewEncoder(outputFile)
	for _, key := range sortedKeys {
		values := intermediateKeyValuePairs[key]

		// User-defined reduce function
		reduceResult := reduceFn(key, values)

		var kv KeyValue = KeyValue{key, reduceResult}
		err := encoder.Encode(&kv)
		if err != nil {
			panic(err)
		}
	}

}
