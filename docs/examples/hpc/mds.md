# DA-MDS (Deterministic Annealing Multi-Dimensional Scaling)

DA-MDS is the implementation of deterministic annealing multi-dimensional scaling algorithm. 
The clustering process pick a subset of sequences which is defined as sample sequence set. 
First, perform the pairwise alignment on and produce a distance matrix. Second, perform the pairwise
clustering and multidimensional scaling on the distance matrix and produce a three dimensional view 
of the sample sequences with different coloring on clusters found from pairwise clustering algorithm. 
Next, refine the clusters found from the previous step to form spatially compact regions known as 
Mega-regions. Then, assign each remaining sequence approximately to a Mega-region which is based on 
the pairwise distance between the sequence and the sequence representing the centers of nodes of 
decomposed tree. For each obtained mega-region, run pairwise alignment and produce the distance matrix.

## DA-MDS Implementation Details

The implementation details of DA-MDS clustering in Twister2 is pictorially represented in Fig.1.

### DataObjectConstants Used by DA-MDS

The constants which are used by the DA-MDS algorithm to specify the number of workers, parallelism, 
dimension, size of matrix (number of rows X number of columns), byte type (either "short" or "big"),
file system, datainput ("generate" or "read"), and config directory. The datainput option is used to 
specify the system whether the user wants to generate the input data or else the system has to read 
the data from the user specified input directory. 

```java
  public static final String WORKERS = "workers";
  public static final String DIMENSIONS = "dim";
  public static final String PARALLELISM_VALUE = "parallelism";
  public static final String DSIZE = "dsize";
  public static final String CSIZE = "csize";
  public static final String DINPUT_DIRECTORY = "dinput";
  public static final String FILE_SYSTEM = "filesys";
  public static final String BYTE_TYPE = "byteType";
  public static final String CONFIG_FILE = "config";
  public static final String DATA_INPUT = "datainput"; //"generate" or "read"
```

### MDSWorker

The entry point for the DA-MDS algorithm is implemented in MDSWorker class. First, it parses the 
command line parameters submitted by the user for running the DA-MDS algorithm. It first sets the 
submitted variables in the JobConfig object and put the JobConfig object into the Twister2Job 
Builder, set the worker class (MDSWorker.java in this example) and submit the job. 

```java
edu.iu.dsc.tws.examples.batch.mds.MDSWorker
```

It is the main class for the DA-MDS which consists of the following tasks namely generation of datapoints, 
partition and read the partitioned data points, and perform the distance calculation between the 
datapoints. It extends the TaskWorker class which has the execute() method, the execute() method 
first invokes the MatrixGenerator class to generate the datapoints in their respective filesystem and 
their directories. Then, the execute() method of MDSWorker invokes "MDSDataProcessingGraph", and 
"MDSComputeProcessingGraph" subsequently. We will briefly discuss the functionalities of each task 
graph defined in the MDSWorker.


### MDSDataProcessingGraph
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

Finally, invoke the taskGraphBuilder to build the first task graph, get the task schedule plan and execution plan for the first task graph, and call the execute() method to execute the datapoints task graph. Once the execution is finished, the output values are retrieved in the "datapointsObject".

```java
      DataFlowTaskGraph dataObjectTaskGraph = mdsDataProcessingGraphBuilder.build();
      //Get the execution plan for the first task graph
      ExecutionPlan plan = taskExecutor.plan(dataObjectTaskGraph);
   
      //Actual execution for the first taskgraph
      taskExecutor.execute(dataObjectTaskGraph, plan);
   
       //Retrieve the output of the first task graph
       DataObject<Object> dataPointsObject = taskExecutor.getOutput(
           dataObjectTaskGraph, plan, "dataobjectsink");

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


#### MDSDataObjectDirectSink

This class receives the message object from the MDSDataObjectSource and write into their respective 
task index values. First, it store the iterator values into the array list then it convert the array
list values into double array values.

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