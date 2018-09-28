# K-Means

## K-Means Clustering

The need to process large amounts of continuously arriving information has led to the exploration and application of big data analytics techniques. Likewise, the painstaking process of clustering numerous datasets containing large numbers of records with high dimensions calls for innovative methods. Traditional sequential clustering algorithms are unable to handle it. They are not scalable in relation to larger sizes of data sets, and they are most often computationally expensive in memory space and time complexities. Yet, the parallelization of data clustering algorithms is paramount when dealing with big data. K-Means clustering is an iterative algorithm hence, it requires a large number of iterative steps to find an optimal solution, and this procedure increases the processing time of clustering. Twister2 provides a dataflow task graph based approach to distribute the tasks in a parallel manner and aggregate the results which reduces the processing time of K-Means Clustering process.

## To run K-Means

### To generate and write the datapoints and centroids in the local filesystem and run the K-Means

```bash
./bin/twister2 submit nodesmpi jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.kmeans.KMeansJobMain -workers 4 -iter 2 -dim 2 -clusters 4 -fname /home/kgovind/input.txt -pointsfile /home/kgovind/kinput.txt -centersfile /home/kgovind/kcentroid.txt -points 100 -filesys local -minvalue 100 -maxvalue 500 -input generate
```

### To generate and write the datapoints and centroids in the HDFS and run the K-Means

```bash
./bin/twister2 submit nodesmi jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.kmeans.KMeansJobMain -workers 4 -iter 2 -dim 2 -clusters 4 -fname /home/kgovind/input.txt -pointsfile /home/kgovind/kinput.txt -centersfile /home/kgovind/kcentroid.txt -points 100 -filesys hdfs -pseedvalue 100 -cseedvalue 200 -input generate
```

## Implementation Details

### KMeansConstants

```text
public static final String ARGS_WORKERS = "workers";

public static final String ARGS_ITR = "iter";

public static final String ARGS_FNAME = "fname";

public static final String ARGS_POINTS = "pointsfile";

public static final String ARGS_CENTERS = "centersfile";

public static final String ARGS_DIMENSIONS = "dim";

public static final String ARGS_CLUSTERS = "clusters";

public static final String ARGS_NUMBER_OF_POINTS = "points";

public static final String ARGS_FILESYSTEM = "filesys"; // "local" or "hdfs"

public static final String ARGS_POINTS_SEED_VALUE = "pseedvalue"; //range for random data points generation

public static final String ARGS_CENTERS_SEED_VALUE = "cseedvalue"; //range for centroids generation

public static final String ARGS_DATA_INPUT = "input"; //"generate" or "read"
```

### KMeansMainJob

The entry point for the K-Means clustering algorithm is implemented in KMeansMainJob

```text
edu.iu.dsc.tws.examples.batch.kmeans.KMeansMainJob
```

It retrieves and parses the command line parameters submitted by the user for running the K-Means Clustering algorithm. It sets the submitted variables in the Configuration object and put the object into the JobConfig and submit it to KMeansJob class.

### KMeansJob

It is the main class for the K-Means clustering which has the following classes namely KMeansSource, KMeansAllReduceTask, and CentroidAggregator.First, the execute method in KMeansJob invokes the KMeansDataGenerator to generate the datapoints file and centroid file, if the user has specified the option ARGS\_DATA\_INPUT as "generate". First, it will invoke the KMeansDataGenerator class and store the generated datapoints and centroids in the respective filesystems which is based on the option ARGS\_FILESYSTEM as "local" or "hdfs". Then, it will invoke the KMeansFileReader to read the input datafile/centroid file either from local filesystem or HDFS.

Next, the datapoints are stored in DataSet \(0th object\) and centroids are stored in DataSet \(1st object\) and call the executor as given below:

```text
taskExecutor.addInput(graph, plan, "source", "points", datapoints);

taskExecutor.addInput(graph, plan, "source", "centroids", centroids);

taskExecutor.execute(graph, plan);
```

This process repeats for ‘N’ number of iterations as specified in the KMeansConstants . For every iteration, the new centroid value is calculated and the calculated value is distributed across all the task instances.

```text
DataSet<Object> dataSet = taskExecutor.getOutput(graph, plan, "sink");

Set<Object> values = dataSet.getData();

for (Object value : values) {
  KMeansCenters kMeansCenters = (KMeansCenters) value;
 centroid = kMeansCenters.getCenters();  
}
```

At the end of every iteration, the centroid value is updated and the iteration continues with the new centroid value.

```text
datapoints.addPartition(0, dataPoint);

centroids.addPartition(1, centroid); 
```

### KMeansSourceTask

The KMeansSourceTask retrieve the input data file and centroid file name, it first calculate the start index and end index which is based on the total data points and the parallelism value as given below:

```text
int startIndex = context.taskIndex() * datapoints.length / context.getParallelism();

int endIndex = startIndex + datapoints.length / context.getParallelism();
```

Then, it calls the KMeansCalculator class to calculate and get the centroid value for the task instance.

```text
kMeansCalculator = new KMeansCalculator(datapoints, centroid,
        context.taskIndex(), 2, startIndex, endIndex);

KMeansCenters kMeansCenters = kMeansCalculator.calculate();
```

Finally, each task instance write their calculated centroids value as given below:

```text
context.writeEnd("all-reduce", kMeansCenters);
```

### KMeansAllReduce Task

The KMeansAllReduceTask retrieve the calculated centroid value in the execute method

```text
  public boolean execute(IMessage message) {
   centroids = ((KMeansCenters) message.getContent()).getCenters();
  }
```

and write the calculated centroid value without the number of datapoints fall into the particular cluster as given below:

```text
  @Override
  public Partition<Object> get() {
   return new Partition<>(context.taskIndex(), new KMeansCenters().setCenters(newCentroids));
  }
```

### CentroidAggregator

The CentroidAggregator implements the IFunction and the function OnMessage which accepts two objects as an argument.

```text
public Object onMessage(Object object1, Object object2)
```

It sums the corresponding centroid values and return the same.

```text
ret.setCenters(newCentroids); 
```

