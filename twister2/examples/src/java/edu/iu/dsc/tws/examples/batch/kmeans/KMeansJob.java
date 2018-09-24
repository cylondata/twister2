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
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataSet;
import edu.iu.dsc.tws.dataset.Partition;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.batch.BaseBatchSink;
import edu.iu.dsc.tws.task.batch.BaseBatchSource;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class KMeansJob extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(KMeansJob.class.getName());

  public static void main(String[] args) {
    LOG.log(Level.INFO, "KMeans job");
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.BasicJobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setName("kmeans-job");
    jobBuilder.setWorkerClass(KMeansJob.class.getName());
    jobBuilder.setRequestResource(new WorkerComputeResource(2, 1024), 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute() {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    //this.jobParameters = KMeansJobParameters.build(config);
    /*int numberOfIterations = this.jobParameters.getIterations();
    String inputFileName = this.jobParameters.getPointFile();
    String centroidFileName = this.jobParameters.getCenterFile();*/

    KMeansSourceTask kMeansSourceTask = new KMeansSourceTask();
    KMeansAllReduceTask kMeansAllReduceTask = new KMeansAllReduceTask();

    TaskGraphBuilder graphBuilder = TaskGraphBuilder.newBuilder(config);
    graphBuilder.addSource("source", kMeansSourceTask, 4);
    ComputeConnection computeConnection = graphBuilder.addSink("sink", kMeansAllReduceTask, 4);
    computeConnection.allreduce("source", "all-reduce", new CentroidAggregator(), DataType.OBJECT);
    graphBuilder.setMode(OperationMode.BATCH);

    //Store datapoints and centroids
    DataSet<Object> datapoints = new DataSet<>(0);
    DataSet<Object> centroids = new DataSet<>(1);

    //Set the configuration values in the configuration parameters.
    KMeansDataGenerator.generateDataPointsFile("/home/kgovind/hadoop-2.9.0/input.txt",
        100, 2, 100, 500);
    KMeansDataGenerator.generateCentroidFile("/home/kgovind/hadoop-2.9.0/centroids.txt",
        4, 2, 100, 500);

    //Reading File
    KMeansFileReader kMeansFileReader = new KMeansFileReader();
    double[][] dataPoint = kMeansFileReader.readDataPoints(
        "/home/kgovind/hadoop-2.9.0/kmeans-input.txt", 2);
    double[][] centroid = kMeansFileReader.readCentroids(
        "/home/kgovind/hadoop-2.9.0/kmeans-centroid.txt", 2, 4);

    DataFlowTaskGraph graph = graphBuilder.build();

    for (int i = 0; i < 2; i++) {
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
        LOG.info("%%% New centroid Values Received: %%%" + Arrays.deepToString(centroid));
      }
    }
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
      LOG.log(Level.INFO, "Received input: " + name);
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
      LOG.log(Level.FINE, "Received message: " + message.getContent());
      centroids = ((KMeansCenters) message.getContent()).getCenters();
      newCentroids = new double[centroids.length][centroids[0].length - 1];
      for (int i = 0; i < centroids.length; i++) {
        for (int j = 0; j < centroids[0].length - 1; j++) {
          double newVal =  centroids[i][j] / centroids[i][centroids[0].length - 1];
          newCentroids[i][j] = newVal;
        }
      }
      LOG.fine("New Centroid Values:" + Arrays.deepToString(newCentroids));
      return true;
    }

    @Override
    public Partition<Object> get() {
      return  new Partition<>(context.taskIndex(), new KMeansCenters().setCenters(newCentroids));
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
