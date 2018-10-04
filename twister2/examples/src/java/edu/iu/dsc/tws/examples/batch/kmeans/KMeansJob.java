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
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataSet;
import edu.iu.dsc.tws.dataset.Partition;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.batch.BaseBatchSink;
import edu.iu.dsc.tws.task.batch.BaseBatchSource;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class KMeansJob extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(KMeansJob.class.getName());

  private KMeansJobParameters kMeansJobParameters;

  @SuppressWarnings("unchecked")
  @Override
  public void execute() {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    KMeansSourceTask kMeansSourceTask = new KMeansSourceTask();
    KMeansAllReduceTask kMeansAllReduceTask = new KMeansAllReduceTask();

    TaskGraphBuilder graphBuilder = TaskGraphBuilder.newBuilder(config);
    graphBuilder.addSource("source", kMeansSourceTask, 4);
    ComputeConnection computeConnection = graphBuilder.addSink("sink", kMeansAllReduceTask, 4);
    computeConnection.allreduce("source", "all-reduce", new CentroidAggregator(), DataType.OBJECT);
    graphBuilder.setMode(OperationMode.BATCH);

    this.kMeansJobParameters = KMeansJobParameters.build(config);

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
        + "\tnumber of clusters:" + noOfClusters + "\tdatapoints file:" + dataPointsFile + workerId
        + "\tcenters file:" + centroidFile + workerId + "\tfilesys:" + fileSystem);

    KMeansFileReader kMeansFileReader = new KMeansFileReader(config, fileSystem);
    if ("generate".equals(inputData)) {
      KMeansDataGenerator.generateDataPointsFile(
          dataPointsFile + workerId, noOfPoints, dimension, dataSeedValue, config,
          fileSystem);
      KMeansDataGenerator.generateCentroidFile(
          centroidFile + workerId, noOfClusters, dimension, centroidSeedValue, config,
          fileSystem);
    }

    double[][] dataPoint = kMeansFileReader.readDataPoints(dataPointsFile + workerId, dimension);
    double[][] centroid = kMeansFileReader.readCentroids(centroidFile + workerId, dimension,
        noOfClusters);

    DataFlowTaskGraph graph = graphBuilder.build();

    //Store datapoints and centroids
    DataSet<Object> datapoints = new DataSet<>(0);
    DataSet<Object> centroids = new DataSet<>(1);

    for (int i = 0; i < iterations; i++) {
      datapoints.addPartition(0, dataPoint);
      centroids.addPartition(1, centroid);

      ExecutionPlan plan = taskExecutor.plan(graph);

      taskExecutor.addInput(graph, plan, "source", "points", datapoints);
      taskExecutor.addInput(graph, plan, "source", "centroids", centroids);
      taskExecutor.execute(graph, plan);

      DataSet<Object> dataSet = taskExecutor.getOutput(graph, plan, "sink");
      Set<Object> values = dataSet.getData();
      for (Object value : values) {
        KMeansCenters kMeansCenters = (KMeansCenters) value;
        centroid = kMeansCenters.getCenters();
      }
    }

    LOG.info("%%% Final Centroid Values Received: %%%" + Arrays.deepToString(centroid));
    //To write the final value into the file or hdfs
    KMeansOutputWriter.writeToOutputFile(centroid, outputFile + workerId, config, fileSystem);
  }

  private static class KMeansSourceTask extends BaseBatchSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private DataSet<Object> input;

    private double[][] centroid = null;
    private double[][] datapoints = null;

    private KMeansCalculator kMeansCalculator = null;

    @Override
    public void execute() {

      int startIndex = context.taskIndex() * datapoints.length / context.getParallelism();
      int endIndex = startIndex + datapoints.length / context.getParallelism();

      LOG.fine("Original Centroid Value::::" + Arrays.deepToString(centroid));

      kMeansCalculator = new KMeansCalculator(datapoints, centroid,
          context.taskIndex(), 2, startIndex, endIndex);
      KMeansCenters kMeansCenters = kMeansCalculator.calculate();

      LOG.fine("Task Index:::" + context.taskIndex() + "\t"
          + "Calculated Centroid Value::::" + Arrays.deepToString(centroid));
      context.writeEnd("all-reduce", kMeansCenters);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void add(String name, DataSet<Object> data) {
      LOG.log(Level.FINE, "Received input: " + name);
      input = data;
      int id = input.getId();

      if (id == 0) {
        Set<Object> dataPoints = input.getData();
        this.datapoints = (double[][]) dataPoints.iterator().next();
      }

      if (id == 1) {
        Set<Object> centroids = input.getData();
        this.centroid = (double[][]) centroids.iterator().next();
      }
    }
  }

  private static class KMeansAllReduceTask extends BaseBatchSink implements Collector<Object> {
    private static final long serialVersionUID = -5190777711234234L;

    private double[][] centroids;
    private double[][] newCentroids;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received centroids: " + context.getWorkerId()
          + ":" + context.taskId());
      centroids = ((KMeansCenters) message.getContent()).getCenters();
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
    public Partition<Object> get() {
      return new Partition<>(context.taskIndex(), new KMeansCenters().setCenters(newCentroids));
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

      KMeansCenters kMeansCenters = (KMeansCenters) object1;
      KMeansCenters kMeansCenters1 = (KMeansCenters) object2;

      LOG.fine("Kmeans Centers 1:" + Arrays.deepToString(kMeansCenters.getCenters()));
      LOG.fine("Kmeans Centers 2:" + Arrays.deepToString(kMeansCenters1.getCenters()));

      KMeansCenters ret = new KMeansCenters();

      double[][] newCentroids = new double[kMeansCenters.getCenters().length]
          [kMeansCenters.getCenters()[0].length];

      if (kMeansCenters.getCenters().length != kMeansCenters1.getCenters().length) {
        throw new RuntimeException("Center sizes not equal " + kMeansCenters.getCenters().length
            + " != " + kMeansCenters1.getCenters().length);
      }

      for (int j = 0; j < kMeansCenters.getCenters().length; j++) {
        for (int k = 0; k < kMeansCenters.getCenters()[0].length; k++) {
          double newVal = kMeansCenters.getCenters()[j][k] + kMeansCenters1.getCenters()[j][k];
          newCentroids[j][k] = newVal;
        }
      }
      ret.setCenters(newCentroids);
      LOG.fine("Kmeans Centers final:" + Arrays.deepToString(newCentroids));
      return ret;
    }
  }

}
