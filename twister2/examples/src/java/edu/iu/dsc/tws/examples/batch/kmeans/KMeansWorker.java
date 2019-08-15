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
package edu.iu.dsc.tws.examples.batch.kmeans;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
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
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.task.TaskEnvironment;
import edu.iu.dsc.tws.task.dataobjects.DataFileReplicatedReadSource;
import edu.iu.dsc.tws.task.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.TaskExecutor;
import edu.iu.dsc.tws.task.impl.TaskGraphBuilder;

/**
 * It is the main class for the K-Means clustering which consists of four main tasks namely
 * generation of datapoints and centroids, partition and read the partitioned data points,
 * read the centroids, and finally perform the distance calculation.
 */
public class KMeansWorker implements IWorker {
  private static final Logger LOG = Logger.getLogger(KMeansWorker.class.getName());

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
  public void execute(Config config,
                      int workerId,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {

    LOG.log(Level.FINE, "Task worker starting: " + workerId);

    TaskEnvironment taskEnv = TaskEnvironment.init(config, workerId, workerController,
        volatileVolume);
    TaskExecutor taskExecutor = taskEnv.getTaskExecutor();

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
    DataFlowTaskGraph datapointsTaskGraph = buildDataPointsTG(dataDirectory, dsize,
        parallelismValue, dimension, config);

    /* Second Graph to read the centroids **/
    DataFlowTaskGraph centroidsTaskGraph = buildCentroidsTG(centroidDirectory, csize,
        parallelismValue, dimension, config);

    /* Third Graph to do the actual calculation **/
    DataFlowTaskGraph kmeansTaskGraph = buildKMeansTG(parallelismValue, config);

    //Get the execution plan before executing the dependent task graphs
    Map<String, ExecutionPlan> taskSchedulePlanMap =
        taskEnv.build(datapointsTaskGraph, centroidsTaskGraph, kmeansTaskGraph);

    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskSchedulePlanMap.get(
        datapointsTaskGraph.getGraphName());

    //Actual execution for the first taskgraph
    taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);

    //Retrieve the output of the first task graph
    DataObject<Object> dataPointsObject = taskExecutor.getOutput(
        datapointsTaskGraph, firstGraphExecutionPlan, "datapointsink");

    //Get the execution plan for the second task graph
    //ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(centroidsTaskGraph);
    ExecutionPlan secondGraphExecutionPlan = taskSchedulePlanMap.get(
        centroidsTaskGraph.getGraphName());

    //Actual execution for the second taskgraph
    taskExecutor.execute(centroidsTaskGraph, secondGraphExecutionPlan);

    //Retrieve the output of the first task graph
    DataObject<Object> centroidsDataObject = taskExecutor.getOutput(
        centroidsTaskGraph, secondGraphExecutionPlan, "centroidsink");

    long endTimeData = System.currentTimeMillis();

    //Perform the iterations from 0 to 'n' number of iterations
    ExecutionPlan plan = taskSchedulePlanMap.get(kmeansTaskGraph.getGraphName());

    for (int i = 0; i < iterations; i++) {
      //add the datapoints and centroids as input to the kmeanssource task.
      taskExecutor.addInput(
          kmeansTaskGraph, plan, "kmeanssource", "points", dataPointsObject);
      taskExecutor.addInput(
          kmeansTaskGraph, plan, "kmeanssource", "centroids", centroidsDataObject);
      //actual execution of the third task graph
      taskExecutor.itrExecute(kmeansTaskGraph, plan);
      //retrieve the new centroid value for the next iterations
      centroidsDataObject = taskExecutor.getOutput(kmeansTaskGraph, plan, "kmeanssink");
    }
    taskExecutor.waitFor(kmeansTaskGraph, plan);
    taskEnv.close();

    if (workerId == 0) {
      DataPartition<?> centroidPartition = centroidsDataObject.getPartition(workerId);
      double[][] centroid = null;
      if (centroidPartition.getConsumer().hasNext()) {
        centroid = (double[][]) centroidPartition.getConsumer().next();
      }
      long endTime = System.currentTimeMillis();

      LOG.info("Total Time : " + (endTime - startTime)
          + "\tData Load time : " + (endTimeData - startTime)
          + "\tCompute Time : " + (endTime - endTimeData));
      LOG.info("Final Centroids After\t" + iterations + "\titerations\t"
          + Arrays.deepToString(centroid));
    }
  }

  public static DataFlowTaskGraph buildDataPointsTG(String dataDirectory, int dsize,
                                                    int parallelismValue, int dimension,
                                                    Config conf) {
    DataObjectSource dataObjectSource = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory);
    KMeansDataObjectCompute dataObjectCompute = new KMeansDataObjectCompute(
        Context.TWISTER2_DIRECT_EDGE, dsize, parallelismValue, dimension);
    KMeansDataObjectDirectSink dataObjectSink = new KMeansDataObjectDirectSink("points");
    TaskGraphBuilder datapointsTaskGraphBuilder = TaskGraphBuilder.newBuilder(conf);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    datapointsTaskGraphBuilder.addSource("datapointsource", dataObjectSource,
        parallelismValue);
    ComputeConnection datapointComputeConnection = datapointsTaskGraphBuilder.addCompute(
        "datapointcompute", dataObjectCompute, parallelismValue);
    ComputeConnection firstGraphComputeConnection = datapointsTaskGraphBuilder.addSink(
        "datapointsink", dataObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    datapointComputeConnection.direct("datapointsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    firstGraphComputeConnection.direct("datapointcompute")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    datapointsTaskGraphBuilder.setMode(OperationMode.BATCH);
    datapointsTaskGraphBuilder.setTaskGraphName("datapointsTG");

    //Build the first taskgraph
    return datapointsTaskGraphBuilder.build();
  }

  public static DataFlowTaskGraph buildCentroidsTG(String centroidDirectory, int csize,
                                                   int parallelismValue, int dimension,
                                                   Config conf) {
    DataFileReplicatedReadSource dataFileReplicatedReadSource
        = new DataFileReplicatedReadSource(Context.TWISTER2_DIRECT_EDGE, centroidDirectory);
    KMeansDataObjectCompute centroidObjectCompute = new KMeansDataObjectCompute(
        Context.TWISTER2_DIRECT_EDGE, csize, dimension);
    KMeansDataObjectDirectSink centroidObjectSink = new KMeansDataObjectDirectSink("centroids");
    TaskGraphBuilder centroidsTaskGraphBuilder = TaskGraphBuilder.newBuilder(conf);

    //Add source, compute, and sink tasks to the task graph builder for the second task graph
    centroidsTaskGraphBuilder.addSource("centroidsource", dataFileReplicatedReadSource,
        parallelismValue);
    ComputeConnection centroidComputeConnection = centroidsTaskGraphBuilder.addCompute(
        "centroidcompute", centroidObjectCompute, parallelismValue);
    ComputeConnection secondGraphComputeConnection = centroidsTaskGraphBuilder.addSink(
        "centroidsink", centroidObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    centroidComputeConnection.direct("centroidsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    secondGraphComputeConnection.direct("centroidcompute")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    centroidsTaskGraphBuilder.setMode(OperationMode.BATCH);
    centroidsTaskGraphBuilder.setTaskGraphName("centTG");

    //Build the second taskgraph
    return centroidsTaskGraphBuilder.build();
  }

  public static DataFlowTaskGraph buildKMeansTG(int parallelismValue, Config conf) {
    KMeansSourceTask kMeansSourceTask = new KMeansSourceTask();
    KMeansAllReduceTask kMeansAllReduceTask = new KMeansAllReduceTask();
    TaskGraphBuilder kmeansTaskGraphBuilder = TaskGraphBuilder.newBuilder(conf);

    //Add source, and sink tasks to the task graph builder for the third task graph
    kmeansTaskGraphBuilder.addSource("kmeanssource", kMeansSourceTask, parallelismValue);
    ComputeConnection kMeanscomputeConnection = kmeansTaskGraphBuilder.addSink(
        "kmeanssink", kMeansAllReduceTask, parallelismValue);

    //Creating the communication edges between the tasks for the third task graph
    kMeanscomputeConnection.allreduce("kmeanssource")
        .viaEdge("all-reduce")
        .withReductionFunction(new CentroidAggregator())
        .withDataType(MessageTypes.OBJECT);
    kmeansTaskGraphBuilder.setMode(OperationMode.BATCH);
    kmeansTaskGraphBuilder.setTaskGraphName("kmeansTG");

    //Build the kmeans taskgraph
    return kmeansTaskGraphBuilder.build();
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

      DataPartition<?> dataPartition = dataPointsObject.getPartition(context.taskIndex());
      datapoints = (double[][]) dataPartition.getConsumer().next();

      DataPartition<?> centroidPartition = centroidsObject.getPartition(context.taskIndex());
      centroid = (double[][]) centroidPartition.getConsumer().next();

      kMeansCalculator = new KMeansCalculator(datapoints, centroid, dim);
      double[][] kMeansCenters = kMeansCalculator.calculate();
      context.writeEnd("all-reduce", kMeansCenters);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void add(String name, DataObject<?> data) {
      //LOG.log(Level.INFO, "Received input: " + name);
      if ("points".equals(name)) {
        this.dataPointsObject = data;
      }
      if ("centroids".equals(name)) {
        this.centroidsObject = data;
      }
    }

    @Override
    public Set<String> getReceivableNames() {
      Set<String> inputKeys = new HashSet<>();
      inputKeys.add("points");
      inputKeys.add("centroids");
      return inputKeys;
    }
  }

  public static class KMeansAllReduceTask extends BaseSink implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    private double[][] centroids;
    private double[][] newCentroids;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.FINE, "Received centroids: " + context.getWorkerId()
          + ":" + context.globalTaskId());
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
    public Set<String> getCollectibleNames() {
      return Collections.singleton("centroids");
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
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

