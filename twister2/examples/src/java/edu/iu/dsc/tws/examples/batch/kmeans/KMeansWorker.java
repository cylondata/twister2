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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.dataobjects.DataFileReplicatedReadSource;
import edu.iu.dsc.tws.api.dataobjects.DataObjectSink;
import edu.iu.dsc.tws.api.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

/**
 * It is the main class for the K-Means clustering which consists of four main tasks namely
 * generation of datapoints and centroids, partition and read the partitioned data points,
 * read the centroids, and finally perform the distance calculation.
 */
public class KMeansWorker extends TaskWorker {
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
  public void execute() {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    KMeansWorkerParameters kMeansJobParameters = KMeansWorkerParameters.build(config);
    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    KMeansWorkerUtils workerUtils = new KMeansWorkerUtils(config);

    int parallelismValue = kMeansJobParameters.getParallelismValue();
    int dimension = kMeansJobParameters.getDimension();
    int numFiles = kMeansJobParameters.getNumFiles();
    int dsize = kMeansJobParameters.getDsize();
    int csize = kMeansJobParameters.getCsize();
    int iterations = kMeansJobParameters.getIterations();
    int workers = kMeansJobParameters.getWorkers();

    String dinputDirectory = kMeansJobParameters.getDatapointDirectory();
    String cinputDirectory = kMeansJobParameters.getCentroidDirectory();

    if (workerId == 0) {
      workerUtils.generateDatapoints(dimension, numFiles, dsize, csize,
          dinputDirectory, cinputDirectory);
    }

    /* First Graph to partition and read the partitioned data points **/
    DataObjectSource sourceTask = new DataObjectSource("direct");
    DataObjectSink sinkTask = new DataObjectSink();
    taskGraphBuilder.addSource("datapointsource", sourceTask, parallelismValue);
    ComputeConnection firstGraphComputeConnection = taskGraphBuilder.addSink(
        "datapointsink", sinkTask, parallelismValue);
    firstGraphComputeConnection.direct("datapointsource", "direct", DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph datapointsTaskGraph = taskGraphBuilder.build();
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
    taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);
    DataObject<Object> dataPointsObject = taskExecutor.getOutput(
        datapointsTaskGraph, firstGraphExecutionPlan, "datapointsink");

    /* Second Graph to read the centroids **/
    DataFileReplicatedReadSource centroidSourceTask = new DataFileReplicatedReadSource("direct");
    DataObjectSink centroidSinkTask = new DataObjectSink();
    taskGraphBuilder.addSource("centroidsource", centroidSourceTask, parallelismValue);
    ComputeConnection secondGraphComputeConnection = taskGraphBuilder.addSink(
        "centroidsink", centroidSinkTask, parallelismValue);
    secondGraphComputeConnection.direct("centroidsource", "direct", DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph centroidsTaskGraph = taskGraphBuilder.build();
    ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(centroidsTaskGraph);
    taskExecutor.execute(centroidsTaskGraph, secondGraphExecutionPlan);

    DataObject<Object> centroidsDataObject = taskExecutor.getOutput(
        centroidsTaskGraph, secondGraphExecutionPlan, "centroidsink");

    /*DataPartition<Object> centroidsDataPartition = centroidsDataObject.getPartitions()[0];
    double[][] centroid1 = workerUtils.getCentroids(centroidsDataPartition.getPartitionId(),
        centroidsDataPartition, csize, dimension);
    LOG.info("Initial Centroid Values are:" + Arrays.deepToString(centroid1));*/

    /* Third Graph to do the actual calculation **/
    KMeansSourceTask kMeansSourceTask = new KMeansSourceTask();
    KMeansAllReduceTask kMeansAllReduceTask = new KMeansAllReduceTask();
    taskGraphBuilder.addSource("kmeanssource", kMeansSourceTask, parallelismValue);
    ComputeConnection computeConnection = taskGraphBuilder.addSink(
        "kmeanssink", kMeansAllReduceTask, parallelismValue);
    computeConnection.allreduce("kmeanssource", "all-reduce",
        new CentroidAggregator(), DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);
    DataFlowTaskGraph kmeansTaskGraph = taskGraphBuilder.build();

    //Store datapoints and centroids
    DataObject<double[][]> centroids = new DataObjectImpl<>(config);

    double[][] centroid = null;
    for (int i = 0; i < iterations; i++) {
      //centroids.addPartition(new EntityPartition<>(0, centroid));

      ExecutionPlan plan = taskExecutor.plan(kmeansTaskGraph);
      taskExecutor.addInput(
          kmeansTaskGraph, plan, "kmeanssource", "points", dataPointsObject);
      taskExecutor.addInput(
          kmeansTaskGraph, plan, "kmeanssource", "centroids", centroidsDataObject);
      //taskExecutor.addInput(kmeansTaskGraph, plan, "kmeanssource", "centroids", centroids);
      taskExecutor.execute(kmeansTaskGraph, plan);

      DataObject<double[][]> dataSet = taskExecutor.getOutput(kmeansTaskGraph, plan, "kmeanssink");
      DataPartition<double[][]> values = dataSet.getPartitions()[0];
      centroid = values.getConsumer().next();
    }
    LOG.info("Final Centroid Values are:" + Arrays.deepToString(centroid));
  }

  private static class KMeansSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private double[][] centroid = null;
    private double[][] datapoints = null;

    private KMeansCalculator kMeansCalculator = null;
    private KMeansWorkerUtils workerUtils = null;

    private DataObject<?> dataPointsObject = null;
    private DataObject<?> dataObject = null;
    private EntityPartition<Object> datapointsEntityPartition = null;

    private DataObject<?> centroidsObject = null;
    private DataObject<?> centroidObject = null;
    private EntityPartition<Object> centroidsEntityPartition = null;

    @Override
    public void execute() {
      workerUtils = new KMeansWorkerUtils(config);
      int dim = Integer.parseInt(config.getStringValue("dim"));

      datapointsEntityPartition
          = (EntityPartition<Object>) dataPointsObject.getPartitions(context.taskIndex());
      if (datapointsEntityPartition != null) {
        dataObject = (DataObject<?>) datapointsEntityPartition.getConsumer().next();
        datapoints = workerUtils.getDataPoints(context.taskIndex(), dataObject);
      }

      centroidsEntityPartition
          = (EntityPartition<Object>) centroidsObject.getPartitions(context.taskIndex());
      if (centroidsEntityPartition != null) {
        centroidObject = (DataObject<?>) centroidsEntityPartition.getConsumer().next();
        centroid = workerUtils.getCentroids(context.taskIndex(), centroidObject);
      }
      kMeansCalculator = new KMeansCalculator(datapoints, centroid, dim);
      double[][] kMeansCenters = kMeansCalculator.calculate();
      context.writeEnd("all-reduce", kMeansCenters);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void add(String name, DataObject<?> data) {
      LOG.log(Level.INFO, "Received input: " + name);
      if ("points".equals(name)) {
        this.dataPointsObject = data;
      }

      if ("centroids".equals(name)) {
        this.centroidsObject = data;
      }
    }
  }

  private static class KMeansAllReduceTask extends BaseSink implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    private double[][] centroids;
    private double[][] newCentroids;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.FINE, "Received centroids: " + context.getWorkerId()
          + ":" + context.taskId());
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
  }

  /**
   * This class aggregates the cluster centroid values and sum the new centroid values.
   */
  public class CentroidAggregator implements IFunction {
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

