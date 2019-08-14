---
id: mds
title: MDS
sidebar_label: MDS
---

# DA-MDS (Deterministic Annealing Multi-Dimensional Scaling)

DA-MDS is the implementation of deterministic annealing multi-dimensional scaling algorithm. 
The clustering process pick a subset of sequences which is defined as sample sequence set. 
First, perform the pairwise alignment on and produce a distance matrix. Second, perform the pairwise
clustering and multidimensional scaling on the distance matrix and produce a three dimensional view 
of the sample sequences with different coloring on clusters found from pairwise clustering algorithm. 
Third, it refine the clusters found from the previous step to form spatially compact regions known as 
Mega-regions. Then, assign each remaining sequence approximately to a Mega-region which is based on 
the pairwise distance between the sequence and the sequence representing the centers of nodes of 
decomposed tree. For each obtained mega-region, run pairwise alignment and produce the distance matrix.

## MDS Implementation Details

The implementation details of DA-MDS application is explained below which consists of two parts namely 
data processing and compute processing. The data processing uses the Twister2 file system to partition 
and read the partitioned data points based on the task parallelism. The compute processing purely 
depends on the MPI to perform the computation.

### DataObjectConstants Used by DA-MDS

The constants which are used by the DA-MDS algorithm to specify the number of workers, parallelism, 
dimension, size of matrix (dsize X dim), byte type (either "short" or "big"),
file system, datainput ("generate" or "read"), and config directory. The datainput option is used to 
specify the system whether the user wants to generate the input data or else the system has to read 
the data from the user specified input directory. 

```java
  public static final String WORKERS = "workers";
  public static final String DIMENSIONS = "dim";
  public static final String PARALLELISM_VALUE = "parallelism";
  public static final String DSIZE = "dsize";
  public static final String DINPUT_DIRECTORY = "dinput";
  public static final String FILE_SYSTEM = "filesys";
  public static final String BYTE_TYPE = "byteType";
  public static final String CONFIG_FILE = "config";
  public static final String DATA_INPUT = "datainput"; //"generate" or "read"
```

### MDSWorker
It is the main class for the DA-MDS which consists of the following tasks namely generation of datapoints, 
partition and read the partitioned data points, and perform the distance calculation between the 
datapoints.  First, it parses the command line parameters submitted by the user for running the DA-MDS 
algorithm. It first sets the submitted variables in the JobConfig object and put the JobConfig object 
into the Twister2Job Builder, set the worker class (MDSWorker.java in this example) and submit the job. 

```java
edu.iu.dsc.tws.examples.batch.mds.MDSWorker
```

It extends the TaskWorker class which has the execute() method, the execute() method 
first invokes the MatrixGenerator class to generate the datapoints in their respective filesystem and 
their directories. Then, the execute() method of MDSWorker invokes "MDSDataProcessingGraph", and 
"MDSComputeProcessingGraph" subsequently. We will briefly discuss the functionalities of each task 
graph defined in the MDSWorker.


### MDSDataProcessingGraph
This is the first task graph to partition and read the partitioned data points. 

```java
    /* First Graph to partition and read the partitioned data points **/
    MDSDataObjectSource mdsDataObjectSource = new MDSDataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        directory, datasize);
    MDSDataObjectSink mdsDataObjectSink = new MDSDataObjectSink(matrixColumLength);
    TaskGraphBuilder mdsDataProcessingGraphBuilder = TaskGraphBuilder.newBuilder(config);
    mdsDataProcessingGraphBuilder.setTaskGraphName("MDSDataProcessing");
```

First, add the source and sink tasks to the task graph builder for the first task graph. Then, create 
the communication edges between the tasks for the first task graph.
 
```java
    mdsDataProcessingGraphBuilder.addSource("dataobjectsource", mdsDataObjectSource, parallel);
    ComputeConnection dataObjectComputeConnection = mdsDataProcessingGraphBuilder.addSink(
           "dataobjectsink", mdsDataObjectSink, parallel);
    dataObjectComputeConnection.direct("dataobjectsource")
           .viaEdge(Context.TWISTER2_DIRECT_EDGE)
           .withDataType(DataType.OBJECT);
    mdsDataProcessingGraphBuilder.setMode(OperationMode.BATCH);
```

Next, invoke the computeGraphBuilder to build the data processing task graph, get the task schedule plan 
and execution plan for the dataprocessing task graph, and call the execute() method to execute the 
dataprocessing task graph. Once the execution is finished, the output values are retrieved in the 
"dataobjectsink".

```java
      DataFlowTaskGraph dataObjectTaskGraph = mdsDataProcessingGraphBuilder.build();
      //Get the execution plan for the first task graph
      ExecutionPlan plan = taskExecutor.plan(dataObjectTaskGraph);
   
      //Actual execution for the first taskgraph
      taskExecutor.execute(dataObjectTaskGraph, plan);
   
       //Retrieve the output of the first task graph
       DataObject<Object> dataPointsObject = taskExecutor.getOutput(dataObjectTaskGraph, plan, "dataobjectsink");
```
#### MDSDataObjectSource

This class partition the datapoints which is based on the task parallelism value. In this example,
we are generating the binary data which uses the "BinaryInputPartitioner" to partition the datapoints. 
Finally, write the partitioned datapoints into their respective edges. The BinaryInputPartitioner
partition the datapoints based on the data input block. 
 
```java
 @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    ExecutionRuntime runtime = (ExecutionRuntime) cfg.get(ExecutorContext.TWISTER2_RUNTIME_OBJECT);
    this.source = runtime.createInput(cfg, context, new BinaryInputPartitioner(
        new Path(getDataDirectory()), getDataSize() * Short.BYTES));
  }
```

In the execute method, we are creating the bytebuffer array object which is twice the size of the 
datasize. We get the input split for the corresponding task indexes and get the record from each 
input split and convert it as a short array and write the short array using context.write to the 
sink task.

```java
 @Override
  public void execute() {
    Buffer buffer;
    byte[] line = new byte[getDataSize() * 2];
    ByteBuffer byteBuffer = ByteBuffer.allocate(getDataSize() * 2);
    byteBuffer.order(ByteOrder.BIG_ENDIAN);
    InputSplit inputSplit = source.getNextSplit(context.taskIndex());
    while (inputSplit != null) {
      try {
        while (!inputSplit.reachedEnd()) {
          while (inputSplit.nextRecord(line) != null) {
            byteBuffer.clear();
            byteBuffer.put(line);
            byteBuffer.flip();
            buffer = byteBuffer.asShortBuffer();
            short[] shortArray = new short[getDataSize()];
            ((ShortBuffer) buffer).get(shortArray);
            context.write(getEdgeName(), shortArray);
          }
        }
        inputSplit = null;
      } catch (Exception ioe) {
        throw new RuntimeException("IOException Occured:" + ioe.getMessage());
      }
    }
    context.end(getEdgeName());
  }
```


#### MDSDataObjectDirectSink

This class receives the message object from the MDSDataObjectSource and write into their respective 
task index values. First, it store the iterator values into the array list then it convert the array
list values into short array values.

```java
public boolean execute(IMessage content) {
    List<short[]> values = new ArrayList<>();
    while (((Iterator) content.getContent()).hasNext()) {
      values.add((short[]) ((Iterator) content.getContent()).next());
    }
    LOG.info("Distance Matrix (Row X Column):" + values.size() + "\tX\t" + values.get(0).length);
    dataPoints = new short[values.size() * columnLength];
    int k = 0;
    for (short[] value : values) {
      for (short aValue : value) {
        dataPoints[k] = aValue;
        k = k + 1;
      }
    }
    return true;
}
``` 

Finally, write the appropriate data points into their respective task index values with the entity 
partition values.

```java
 @Override
   public DataPartition<short[]> get() {
     return new EntityPartition<>(context.taskIndex(), dataPoints);
   }
```

### MDSComputeProcessingGraph
The MDS Compute Processing depends on DAMDS and Common package so we have the complete application 
written in Twister2Applications whereas Twister2 example has only data processing part and 
retrieving the output of the data processing. If you want to see the complete example, please go to
the Twister2Applications project in this location.

```
https://github.com/DSC-SPIDAL/twister2applications
```

### MDSSourceTask
The MDSSourceTask receive the datapoints object from the MDSDataProcessingGraph and assign the points
into the DataObject. The execute method get the datapoints as a short array corresponding to the task 
indexes and send those datapoints to the executeMDS method. 

```java
 @Override
 public void execute() {
    DataPartition<?> dataPartition = dataPointsObject.getPartitions(context.taskIndex());
    datapoints = (short[]) dataPartition.getConsumer().next();
    executeMds(datapoints);
    context.writeEnd(Context.TWISTER2_DIRECT_EDGE, "MDS_Execution");
 }
``` 

The executeMDS method invoke the MDSProgramWorker which passes the corresponding data points along 
with the required values such as ParallelOps.threadComm, mdsconfig, byteOrder, BlockSize, and 
mainTimer.

```java
 private void executeMds(short[] datapoints) {
     Stopwatch mainTimer = Stopwatch.createStarted();
     MDSProgramWorker mdsProgramWorker = new MDSProgramWorker(0, ParallelOps.threadComm,
             mdsconfig, byteOrder, BlockSize, mainTimer, null, datapoints);
     try {
         mdsProgramWorker.run();
     } catch (IOException e) {
         throw new RuntimeException("IOException Occured:" + e.getMessage());
        }
     }
```


### MDSProgramWorker
The MDSProgramWorker is the one which actually performs the MDS. The run method in the MDSProgramWorker
first setup the required input parameters to run the MDS application. Next, it read the weights 
using the readWeights method. Then, it sends the corresponding distance matrix and weights
to the calculate statistics method to run the MDS application. 

### Prerequisites

The MDS application in Twister2 requires two external packages namely common and damds. Please 
install those packages before running this application. 

```
https://github.com/DSC-SPIDAL/damds

https://github.com/DSC-SPIDAL/common
```

## Config.Properties
The config.properties is required for running the MDS application. The user has to just leave the 
"DistanceMatrixFile" as blank and specify the pointsfile, timing file, and summary file. Also, the
user has to specify the number of datapoints considered for the test (for example: in this case it
is 1000).


```text
DistanceMatrixFile =
WeightMatrixFile =
LabelFile =
InitialPointsFile =
PointsFile = /tmp/matrix/damds-points.txt
TimingFile = /tmp/matrix/damds-timing.txt
SummaryFile = /tmp/matrix/damds-summary.txt
NumberDataPoints = 1000
TargetDimension = 3
DistanceTransform = 1.0
Threshold = 0.000001 
Alpha = 0.95
TminFactor = 0.5
StressIterations = 1000
CGIterations = 100
CGErrorThreshold = 0.00001
IsSammon = false
IsBigEndian = true
IsMemoryMapped = true
```

## Running MDS Application

```bash
 /home/kannan/twister2/bazel-bin/scripts/package/twister2-0.2.2/bin/twister2 submit standalone jar target/mds-0.2.1-SNAPSHOT-jar-with-dependencies.jar edu.iu.dsc.tws.apps.MDSProgram -dinput /tmp/matrix -filesys local -byteType big -config /home/kannan/twister2applications/twister2/mds/config.properties -datainput generate -workers 4 -parallelism 4 -dsize 1000 -dim 1000
```

### Sample Output 

```text

Distance summary... 
  Count=1000000
  Max=0.999969
  Sum=500482.041627
  Sum of square=333744.962122
  Average=0.500482
  Standard deviation=0.288553
  MissingDistPercentage=0.0

Start of loop 216 Temperature (T_Cur) 0.0000
  Loop 216 Iteration 0 Avg CG count 2.0000 Stress 0.31896
End of loop 216 Total Iterations 1 Avg CG count 2.0000 Stress 0.31896
  Loop 216 Iteration 0 Avg CG count 2.0000 Stress 0.31896
End of loop 216 Total Iterations 1 Avg CG count 2.0000 Stress 0.31896
  Loop 216 Iteration 0 Avg CG count 2.0000 Stress 0.31896
End of loop 216 Total Iterations 1 Avg CG count 2.0000 Stress 0.31896

Finishing DAMDS run ...
Finishing DAMDS run ...
  Total Time: 0d:00H:01M:12S:399mS (72399 ms) Loop Time: 0d:00H:01M:12S:193mS (72193 ms)
  Total Loops: 216
  Total Iterations: 727
  Total CG Iterations: 1599 Avg. CG Iterations: 2.1994
  Final Stress:	0.3189564742212961
  Total Time: 0d:00H:01M:12S:384mS (72384 ms) Loop Time: 0d:00H:01M:12S:195mS (72195 ms)
  Total Loops: 216
  Total Iterations: 727
  Total CG Iterations: 1599 Avg. CG Iterations: 2.1994
  Final Stress:	0.3189564742212961
Finishing DAMDS run ...
  Total Time: 0d:00H:01M:12S:438mS (72438 ms) Loop Time: 0d:00H:01M:12S:193mS (72193 ms)
  Total Loops: 216
  Total Iterations: 727
  Total CG Iterations: 1599 Avg. CG Iterations: 2.1994
  Final Stress:	0.3189564742212961
Finishing DAMDS run ...
  Total Time: 0d:00H:01M:12S:540mS (72540 ms) Loop Time: 0d:00H:01M:12S:184mS (72184 ms)
  Total Loops: 216
  Total Iterations: 727
  Total CG Iterations: 1599 Avg. CG Iterations: 2.1994
  Final Stress:	0.3189564742212961
[2019-06-13 13:23:14 -0400] [INFO] [worker-3] [main] edu.iu.dsc.tws.apps.mds.MDSWorker: Received message:java.util.ArrayList$Itr@5609159b  
[2019-06-13 13:23:14 -0400] [INFO] [worker-1] [main] edu.iu.dsc.tws.apps.mds.MDSWorker: Received message:java.util.ArrayList$Itr@5609159b  
[2019-06-13 13:23:14 -0400] [INFO] [worker-2] [main] edu.iu.dsc.tws.apps.mds.MDSWorker: Received message:java.util.ArrayList$Itr@5609159b  
[2019-06-13 13:23:14 -0400] [INFO] [worker-0] [main] edu.iu.dsc.tws.apps.mds.MDSWorker: Received message:java.util.ArrayList$Itr@7748410a  
[2019-06-13 13:23:14 -0400] [INFO] [worker-0] [main] edu.iu.dsc.tws.apps.mds.MDSWorker: Data Load time : 290
Total Time : 72985Compute Time : 72695  
[2019-06-13 13:23:14 -0400] [INFO] [worker-0] [main] edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorker: Worker finished executing - 0  
[2019-06-13 13:23:14 -0400] [INFO] [worker-2] [main] edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorker: Worker finished executing - 2  
[2019-06-13 13:23:14 -0400] [INFO] [worker-3] [main] edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorker: Worker finished executing - 3  
[2019-06-13 13:23:14 -0400] [INFO] [worker-1] [main] edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorker: Worker finished executing - 1  
[2019-06-13 13:23:14 -0400] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.JobMaster: All 4 workers have completed. JobMaster is stopping.  
```
