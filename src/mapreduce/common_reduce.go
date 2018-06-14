package mapreduce

import(
	"os"

	"encoding/json"
	"log"

)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//

	var KeyValues map[string][]string
	KeyValues = make(map[string][]string)

	//Step1: Open all map-files with the same reduce-number postfix => read kv and aggregate into hashmap
	for i:=0; i<nMap; i++{

	    //1.1 Open File to Read
	    Filename := reduceName(jobName, i, reduceTaskNumber)	    
	    FileToopen, err := os.Open(Filename)
	    if err!=nil{
	        log.Fatal("Fail to open file: %s", FileToopen)
	    }
	    defer FileToopen.Close()
	    
	    //1.2 json decoder 
	    dec := json.NewDecoder(FileToopen)

	    
	    for{ // each loop, proceed with a kv from given file

	        //1.2.1 read a kv
	        var kv KeyValue
	        err := dec.Decode(&kv)
	        if err!=nil{
	            break
	        }

	        //1.2.2 save kv in Hashmap -- KeyValues
	        _, exists := KeyValues[kv.Key]
	        if exists {
	            Values := KeyValues[kv.Key]
	            Values = append(Values, kv.Value)
	        } else {
		    Values := []string{ kv.Value }
	            KeyValues[kv.Key] = Values
	        }
	    }     
	}


	//Step2: for every <key,[values]> do reduceF => save it in mergefile 
	//2.1 Create MergeFile
	Filename := mergeName(jobName, reduceTaskNumber)
	FileToWrite, err := os.Create(Filename)
	if err!=nil{
	    log.Fatal("Fail to create file: %s", FileToWrite)
	}
	defer FileToWrite.Close()	

	//2.2 write kv to MergeFile
	for key, values :=range(KeyValues) {
	    //2.2.1 get result from reduceF
	    res := reduceF(key, values)

	    //2.2.2 write result in json form
	    enc := json.NewEncoder(FileToWrite)
	    err := enc.Encode(&KeyValue{key, res})
	    if err!=nil{
	        log.Fatal("Fail to write kv:%v in file", KeyValue{key, res})
	    }
	}	    

}
