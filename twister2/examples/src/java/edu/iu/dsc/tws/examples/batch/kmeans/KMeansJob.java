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

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

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

public class KMeansJob extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(KMeansJob.class.getName());

  @SuppressWarnings("unchecked")
  @Override
  public void execute() {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    KMeansSourceTask kMeansSourceTask = new KMeansSourceTask();
    KMeansAllReduceTask kMeansAllReduceTask = new KMeansAllReduceTask();

    KMeansJobParameters kMeansJobParameters = KMeansJobParameters.build(config);

    int parallelismValue = kMeansJobParameters.getParallelismValue();

    TaskGraphBuilder graphBuilder = TaskGraphBuilder.newBuilder(config);
    graphBuilder.addSource("source", kMeansSourceTask, parallelismValue);
    ComputeConnection computeConnection = graphBuilder.addSink("sink", kMeansAllReduceTask,
        parallelismValue);
    computeConnection.allreduce("source", "all-reduce", new CentroidAggregator(), DataType.OBJECT);
    graphBuilder.setMode(OperationMode.BATCH);

    int workers = kMeansJobParameters.getWorkers();
    int iterations = kMeansJobParameters.getIterations();
    int noOfPoints = kMeansJobParameters.getNumberOfPoints();
    int dimension = kMeansJobParameters.getDimension();
    int dataSeedValue = kMeansJobParameters.getPointsSeedValue();
    int centroidSeedValue = kMeansJobParameters.getCentroidsSeedValue();
    int noOfClusters = kMeansJobParameters.getClusters();

    String dataPointsFile = kMeansJobParameters.getPointsFile();
    String centroidFile = kMeansJobParameters.getCentersFile();
    String fileSystem = kMeansJobParameters.getFileSystem();
    String inputData = kMeansJobParameters.getDataInput();
    String outputFile = kMeansJobParameters.getFileName();

    LOG.info("workers:" + workers + "\titeration:" + iterations + "\toutfile:" + outputFile
        + "\tnumber of datapoints:" + noOfPoints + "\tdimension:" + dimension
        + "\tnumberofclusters:" + noOfClusters + "\tdatapointsfile:" + dataPointsFile + workerId
        + "\tcenters file:" + centroidFile + workerId + "\tfilesys:" + fileSystem
        + "\tparallelism value:" + parallelismValue);

    KMeansFileReader kMeansFileReader = new KMeansFileReader(config, fileSystem);
    if ("generate".equals(inputData)) {
      KMeansDataGenerator.generateDataPointsFile(
          dataPointsFile + workerId, noOfPoints, dimension, dataSeedValue, config,
          fileSystem);
      KMeansDataGenerator.generateCentroidFile(
          centroidFile + workerId, noOfClusters, dimension, centroidSeedValue, config,
          fileSystem);
    }

    double[][] dataPoint;
    double[][] centroid;
    try {
      dataPoint = kMeansFileReader.readDataPoints(dataPointsFile + workerId, dimension);
      centroid = kMeansFileReader.readCentroids(centroidFile + workerId, dimension, noOfClusters);
    } catch (IOException e) {
      throw new RuntimeException("Data Points Reading Error:", e);
    }

    DataFlowTaskGraph graph = graphBuilder.build();
    //Store datapoints and centroids
    DataObject<double[][]> datapoints = new DataObjectImpl<>(config);
    DataObject<double[][]> centroids = new DataObjectImpl<>(config);

    for (int i = 0; i < iterations; i++) {
      datapoints.addPartition(new EntityPartition<>(0, dataPoint));
      centroids.addPartition(new EntityPartition<>(0, centroid));

      ExecutionPlan plan = taskExecutor.plan(graph);

      taskExecutor.addInput(graph, plan, "source", "points", datapoints);
      taskExecutor.addInput(graph, plan, "source", "centroids", centroids);
      taskExecutor.execute(graph, plan);

      DataObject<double[][]> dataSet = taskExecutor.getOutput(graph, plan, "sink");
      DataPartition<double[][]> values = dataSet.getPartitions()[0];
      centroid = values.getConsumer().next();
    }

    //To write the final value into the local file system or hdfs
    KMeansUtils.writeToOutputFile(centroid, outputFile + workerId, config, fileSystem);
  }

  private static class KMeansSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private double[][] centroid = null;
    private double[][] datapoints = null;
    private KMeansCalculator kMeansCalculator = null;

    @Override
    public void execute() {

      int startIndex = context.taskIndex() * datapoints.length / context.getParallelism();
      int endIndex = startIndex + datapoints.length / context.getParallelism();
      int dim = Integer.parseInt(config.getStringValue("dim"));

      LOG.info("Start index and end index:" + startIndex + "\t" + endIndex);
      kMeansCalculator = new KMeansCalculator(datapoints, centroid, dim, startIndex, endIndex);
      double[][] kMeansCenters = kMeansCalculator.calculate();
      context.writeEnd("all-reduce", kMeansCenters);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void add(String name, DataObject<?> data) {
      LOG.log(Level.FINE, "Received input: " + name);
      if ("points".equals(name)) {
        DataPartition<double[][]> dataPoints = (DataPartition<double[][]>)
            data.getPartitions()[0];
        this.datapoints = dataPoints.getConsumer().next();
      }

      if ("centroids".equals(name)) {
        DataPartition<double[][]> centroids = (DataPartition<double[][]>)
            data.getPartitions()[0];
        this.centroid = centroids.getConsumer().next();
      }
    }
  }

  private static class KMeansAllReduceTask extends BaseSink implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    private double[][] centroids;
    private double[][] newCentroids;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received centroids: " + context.getWorkerId()
          + ":" + context.taskId());
      centroids = (double[][]) message.getContent();
      newCentroids = new double[centroids.length][centroids[0].length - 1];
      for (int i = 0; i < centroids.length; i++) {
        for (int j = 0; j < centroids[0].length - 1; j++) {
          double newVal = centroids[i][j] / centroids[i][centroids[0].length - 1];
          newCentroids[i][j] = newVal;
        }
      }
      LOG.fine("New Centroid Values:" + Arrays.deepToString(newCentroids));
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
