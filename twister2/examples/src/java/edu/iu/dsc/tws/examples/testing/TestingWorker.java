//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.examples.testing;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.task.IFunction;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.api.task.modifiers.Collector;
import edu.iu.dsc.tws.api.task.modifiers.Receptor;
import edu.iu.dsc.tws.api.task.nodes.BaseSink;
import edu.iu.dsc.tws.api.task.nodes.BaseSource;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansCalculator;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansDataObjectDirectSink;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerParameters;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerUtils;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.TaskGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;

public class TestingWorker extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(TestingWorker.class.getName());

  public static DataFlowTaskGraph buildStreamingTaskGraph(String dataDirectory, int dsize,
                                                    int parallelismValue, int dimension,
                                                    Config conf) {
    TestingDataObjectStreamingSource dataObjectSource = new TestingDataObjectStreamingSource(
        Context.TWISTER2_DIRECT_EDGE, dataDirectory);
    TestingDataObjectStreamingSink dataObjectSink = new TestingDataObjectStreamingSink();
    TaskGraphBuilder datapointsTaskGraphBuilder = TaskGraphBuilder.newBuilder(conf);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    datapointsTaskGraphBuilder.addSource("streamingsource", dataObjectSource,
        parallelismValue);
    ComputeConnection firstGraphComputeConnection = datapointsTaskGraphBuilder.addSink(
        "streamingsink", dataObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    firstGraphComputeConnection.direct("streamingsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    datapointsTaskGraphBuilder.setMode(OperationMode.STREAMING);

    datapointsTaskGraphBuilder.setTaskGraphName("streamingTG");

    //Build the first taskgraph
    return datapointsTaskGraphBuilder.build();
  }

  public static DataFlowTaskGraph buildBatchTaskGraph(String dataDirectory, int parallelismValue,
                                                   Config conf) {
    TestingDataObjectBatchSource dataObjectSource = new TestingDataObjectBatchSource(
        Context.TWISTER2_DIRECT_EDGE, dataDirectory);
    KMeansDataObjectDirectSink dataObjectSink = new KMeansDataObjectDirectSink();
    TaskGraphBuilder datapointsTaskGraphBuilder = TaskGraphBuilder.newBuilder(conf);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    datapointsTaskGraphBuilder.addSource("batchsource", dataObjectSource,
        parallelismValue);
    ComputeConnection firstGraphComputeConnection = datapointsTaskGraphBuilder.addSink(
        "batchsink", dataObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    firstGraphComputeConnection.direct("batchsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    datapointsTaskGraphBuilder.setMode(OperationMode.BATCH);

    datapointsTaskGraphBuilder.setTaskGraphName("batchTG");

    //Build the batch taskgraph
    return datapointsTaskGraphBuilder.build();
  }

  /**
   * First, the execute method invokes the generateDataPoints method to generate the datapoints file
   * and centroid file based on the respective filesystem submitted by the user. Next, it invoke
   * the DataObjectSource and DataObjectSink to partition and read the partitioned data points
   * respectively through data points task graph. Then, it calls the DataFileReader to read the
   * centroid values from the filesystem through centroid task graph. Next, the datapoints are
   * stored in DataSet \(0th object\) and centroids are stored in DataSet 1st object\). Finally, it
   * constructs the kmeans task graph to perform the clustering process which computes the distance
   * between the centroids and data points.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void execute() {
    LOG.log(Level.FINE, "Task worker starting: " + workerId);

    KMeansWorkerParameters kMeansJobParameters = KMeansWorkerParameters.build(config);
    KMeansWorkerUtils workerUtils = new KMeansWorkerUtils(config);

    int parallelismValue = kMeansJobParameters.getParallelismValue();
    int dimension = kMeansJobParameters.getDimension();
    int numFiles = kMeansJobParameters.getNumFiles();
    int dsize = kMeansJobParameters.getDsize();
    int csize = kMeansJobParameters.getCsize();
    int iterations = kMeansJobParameters.getIterations();

    String dataDirectory = kMeansJobParameters.getDatapointDirectory() + workerId;
    String centroidDirectory = kMeansJobParameters.getCentroidDirectory() + workerId;

    workerUtils.generateDatapoints(dimension, numFiles, dsize, csize, dataDirectory,
        centroidDirectory);

    long startTime = System.currentTimeMillis();

    /* First Graph to partition and read the partitioned data points **/
    DataFlowTaskGraph datapointsTaskGraph = buildStreamingTaskGraph(dataDirectory, dsize,
        parallelismValue, dimension, config);
    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
    //Actual execution for the first taskgraph
    //taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);
    taskExecutor.iExecute(datapointsTaskGraph, firstGraphExecutionPlan);
    //Retrieve the output of the first task graph
    DataObject<Object> dataPointsObject = taskExecutor.getOutput(
        datapointsTaskGraph, firstGraphExecutionPlan, "datapointsink");


    /* Second Graph to read the centroids **/
    DataFlowTaskGraph centroidsTaskGraph = buildBatchTaskGraph(centroidDirectory, csize,
        parallelismValue, dimension, config);
    //Get the execution plan for the second task graph
    ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(centroidsTaskGraph);
    //Actual execution for the second taskgraph
    taskExecutor.execute(centroidsTaskGraph, secondGraphExecutionPlan);
    //Retrieve the output of the first task graph
    DataObject<Object> centroidsDataObject = taskExecutor.getOutput(
        centroidsTaskGraph, secondGraphExecutionPlan, "centroidsink");

    long endTimeData = System.currentTimeMillis();


//    /* Third Graph to do the actual calculation **/
//    DataFlowTaskGraph kmeansTaskGraph = buildKMeansTG(parallelismValue, config);
//
//    //Perform the iterations from 0 to 'n' number of iterations
//    ExecutionPlan plan = taskExecutor.plan(kmeansTaskGraph);
//    for (int i = 0; i < iterations; i++) {
//      //add the datapoints and centroids as input to the kmeanssource task.
//      taskExecutor.addInput(
//          kmeansTaskGraph, plan, "kmeanssource", "points", dataPointsObject);
//      taskExecutor.addInput(
//          kmeansTaskGraph, plan, "kmeanssource", "centroids", centroidsDataObject);
//      //actual execution of the third task graph
//      taskExecutor.itrExecute(kmeansTaskGraph, plan);
//      //retrieve the new centroid value for the next iterations
//      centroidsDataObject = taskExecutor.getOutput(kmeansTaskGraph, plan, "kmeanssink");
//    }
//    taskExecutor.waitFor(kmeansTaskGraph, plan);
//
//    DataPartition<?> centroidPartition = centroidsDataObject.getPartitions(workerId);
//    double[][] centroid = (double[][]) centroidPartition.getConsumer().next();
//    long endTime = System.currentTimeMillis();
//    if (workerId == 0) {
//      LOG.info("Data Load time : " + (endTimeData - startTime) + "\n"
//          + "Total Time : " + (endTime - startTime)
//          + "Compute Time : " + (endTime - endTimeData));
//    }
//    LOG.fine("Final Centroids After\t" + iterations + "\titerations\t"
//        + Arrays.deepToString(centroid));
  }

  public static class KMeansSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private double[][] centroid = null;
    private double[][] datapoints = null;

    private KMeansCalculator kMeansCalculator = null;
    private DataObject<?> dataPointsObject = null;
    private DataObject<?> centroidsObject = null;

    @Override
    public void execute() {
      int dim = Integer.parseInt(config.getStringValue("dim"));

      DataPartition<?> dataPartition = dataPointsObject.getPartitions(context.taskIndex());
      datapoints = (double[][]) dataPartition.getConsumer().next();

      DataPartition<?> centroidPartition = centroidsObject.getPartitions(context.taskIndex());
      centroid = (double[][]) centroidPartition.getConsumer().next();

      kMeansCalculator = new KMeansCalculator(datapoints, centroid, dim);
      double[][] kMeansCenters = kMeansCalculator.calculate();
      context.writeEnd("all-reduce", kMeansCenters);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void add(String name, DataObject<?> data) {
//      LOG.log(Level.INFO, "Received input: " + name);
      if ("points".equals(name)) {
        this.dataPointsObject = data;
      }
      if ("centroids".equals(name)) {
        this.centroidsObject = data;
      }
    }
  }

  public static class KMeansAllReduceTask extends BaseSink implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    private double[][] centroids;
    private double[][] newCentroids;

    private DataObject<Object> datapoints = null;

    @Override
    public boolean execute(IMessage message) {
//      LOG.log(Level.FINE, "Received centroids: " + context.getWorkerId()
//          + ":" + context.globalTaskId());
      centroids = (double[][]) message.getContent();
      newCentroids = new double[centroids.length][centroids[0].length - 1];
      for (int i = 0; i < centroids.length; i++) {
        for (int j = 0; j < centroids[0].length - 1; j++) {
          double newVal = centroids[i][j] / centroids[i][centroids[0].length - 1];
          newCentroids[i][j] = newVal;
        }
      }
      return true;
    }

    @Override
    public DataPartition<double[][]> get() {
      return new EntityPartition<>(context.taskIndex(), newCentroids);
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
      this.datapoints = new DataObjectImpl<>(config);
    }
  }

  /**
   * This class aggregates the cluster centroid values and sum the new centroid values.
   */
  public static class CentroidAggregator implements IFunction {
    private static final long serialVersionUID = -254264120110286748L;

    /**
     * The actual message callback
     *
     * @param object1 the actual message
     * @param object2 the actual message
     */
    @Override
    public Object onMessage(Object object1, Object object2) throws ArrayIndexOutOfBoundsException {

      double[][] kMeansCenters = (double[][]) object1;
      double[][] kMeansCenters1 = (double[][]) object2;

      double[][] newCentroids = new double[kMeansCenters.length]
          [kMeansCenters[0].length];

      if (kMeansCenters.length != kMeansCenters1.length) {
        throw new RuntimeException("Center sizes not equal " + kMeansCenters.length
            + " != " + kMeansCenters1.length);
      }

      for (int j = 0; j < kMeansCenters.length; j++) {
        for (int k = 0; k < kMeansCenters[0].length; k++) {
          double newVal = kMeansCenters[j][k] + kMeansCenters1[j][k];
          newCentroids[j][k] = newVal;
        }
      }
      return newCentroids;
    }
  }
}

