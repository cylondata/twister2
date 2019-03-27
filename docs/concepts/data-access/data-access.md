### Data Access

Data access abstracts out various data sources including files and streaming sources to simplify the
job of an application developer. Twister2 includes a lower level API for data access 
in addition to a higher level abstraction. For example, the abstraction of a File System allows Twister2 
to support NFS, local file system, and HDFS which enables the developer to store and read data from 
any file by specifying only the URL. 

The important role of the data access layer is to handle data partitioning and data locality in an 
efficient manner. An unbalanced set of data partitions will create stragglers, which will increase 
the execution time of the application. The data access layer is responsible for providing the developer
with appropriate information regarding data locality. Data locality directly affects the execution 
time since unnecessary data movements will degrade the efficiency of the application. In addition 
to the built-in functions of Twister2, the developer is given the option to plug in custom logic to 
handle data partitioning and locality.

For more details, please refer the manuscript

https://onlinelibrary.wiley.com/doi/epdf/10.1002/cpe.5189

### Main components of data access layer

The data access layer consists of the following main components namely

1. Input Split
2. Input Partitioner or Input Formatter
3. Input Split Assigner
4. OutputWriter
5. FileSystem
6. Dataset

We will briefly discuss the functionality of each components as below. 

#### Input Split

The input split provides information on a particular part of a file, possibly located or hosted
on a distributed file system and replicated among several hosts. 

Twister2 supports the following type of input splits namely

1. FileInputSplit - It is abstracted from LocatableInputSplit in which input split refers to split 
   located on one or more hosts.
2. TextInputSplit - It is abstracted from DelimitedInputSplit in which input split refers to split 
   based on the delimited values. 
3. CSVInputSplit - It is also abstracted from DelimitedInputSplit mainly used to split the CSV file.
4. BinaryInputSplit - It is abstracted from FileInputSplit mainly used to split the binary file.  

#### Input Partitioner

The input partitioner describes 
 * how the input data is partitioned based on the task parallelism value 
 * how to read the records from the input split
 
Twister2 supports the various types of input partitioners or formatters namely

1. FileInputPartitioner - It is the abstraction for the file input partitioner. It create the split
   of the input files which is based on the number of total number of bytes divided by the task 
   parallelism value. 
   
2. FixedInputPartitioner - It is really useful if the user exactly know the number of lines
   available in the input data file. It split or partition the files which is based on the total 
   number of lines divided by the task parallelism value.
   
3. CompleteFileInputPartitioner - It is the abstraction for partitioning and reading the complete 
   file without creating the splits. 
   
4. BinaryInputPartitioner - It is used to split the binary input file. 
     
#### Input Split Assigner

The input split assigner distributes the input splits among the task instances of the data source 
exists. It also return the next input split that shall be consumed. The consumer's host is passed as 
a parameter to allow localized assignments. Twister2 supports two types of input split assigners 
namely 

1. OrderedInputSplitAssigner - It assigns the input splits based on the partitions and key value.
2. LocatableInputSplitAssigner - It assigns to each host splits that are local, before assigning 
   splits that are not local.

#### OutputWriter

The output writer is mainly responsible for writing the output to the file. Twister2 has the 
TextOutputWriter which abstracts from the FileOutputWriter for writing the data to the local file 
system or to the distributed file system(HDFS).

#### FileSystem

The file system is an abstraction of generic file system which can be extended to support both
distributed file system or a local file system. Twister2 file system supports the following
operations namely

1. File I/O Operations for both HDFS and local file system
2. BlockLocation for both HDFS and local file system 
3. FileStatus for both HDFS and local file system
4. File Path operations for both HDFS and local file system

#### DataSet

It represents the distributed data set. It has the partition of a distributed data set. Also, it
has the functionality to split and retrieve the partition of a distributed data set.   

#### Twister2 and HDFS 

The Hadoop Distributed File System (HDFS) (https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) 
is a high fault-tolerant distributed file system. It provides high throughput access to the data 
which is suitable for the applications require large datasets. It supports the master/slave architecture
which consists of namenode and datanode. Twister2 has the interface to connect the HDFS using 
the Hadoop/HDFS API's.


    

