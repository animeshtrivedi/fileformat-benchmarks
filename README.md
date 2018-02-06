# Standalone Java File Format Benchmarks  

This repo contains stand alone java program to benchmark read, projection, and selectivity 
performance of Parquet, ORC, Arrow, JSON, and Avro file formats on HDFS. It uses the modern 
vector API for columnar formats. 
 
 
This source code is released under Apache license. 

## Setting up and compilation 

Please clone the repo and execute 
 ```bash
 mvn -DskipTests -T 1C  install
 ```
That will give you a stand alone jar at `./target/filebench-1.0.jar`

## How to generate datasets 
#### TPC-DS dataset 
TPC-DS data set can be generated using Databricks's tool at 
https://github.com/databricks/spark-sql-perf

Please follow the instruction from there.
 
#### Any arbitrary dataset
You can use `parquet-generator` toolset from https://github.com/zrlio/parquet-generator. The name is 
a bit misleading as it can generate dataset in any format. 

Please follow the instructions from there. 

## How to run 

To run the benchmark you need to provide right classpath with correct jars. As starters, you need to 
provide the HDFS classpath, and all other locations contains the right jars. HDFS distribution does 
not include ORC and Arrow jars. They can be downloaded from the maven central. 
 
```bash
java -cp your_class_path com.github.animeshtrivedi.FileBench.Main [args]
```

The arguments you can pass: 
```bash
 -h,--help                 show help.
 -i,--input <arg>          [String] input directory location on an HDFS-compatible
                           fs.
 -p,--parallel <arg>       [Int] parallel instances.
 -P,--projectivity <arg>   [Int] projectivity on the 0th int column.
 -S,--selection <arg>      [Int] selectivity on the 0th int column.
 -t,--test <arg>           [String] test.
 -w,--warmupinput <arg>    [String] same as the input but for the warmup run.
```

`-i` is the input top-level directory where the data files are stored. 

`-p` is the number of parallel instances the benchmark should run. The benchmark enumerates all files in the input
 location and gives 1 file to each instance. So you if you want 16 parallel instances, then you should divide 
 your input data in 16 parallel files. 
 
 `-P` what projectivity to use, in percentage. This is a specific test only valid for 100 column integer table generated
 from the `parquet-generator`. Since the table has 100 column, k% projectivity will choose k columns to read. 
 
 `-S` what selectivity predicate to use. The first column of the same 100 column integer table contains values in between 
  0 and 99. The selectivity takes a value where only rows which are <= than the passed values are read. This is to 
   test the filter performance. It also only works with the 100 column table. 
 
 `-t` test names. Valid names are: parquetread, orcread, arrowread, jsonread, avroread, hdfsread, and pq2arrow. The 
    last one is a special test that actually converts a Parquet dataset into an Arrow dataset, and saves it at 
     `\arrowOutput` directory. 
  
  `-w` warmup input location. Warm up test is the same test which is executed on the warmup input location instead of 
  the actual input location. After the warmup, the actual test is run. 
   

## Comments or Suggestions 

Please open a git issue and let me know. 