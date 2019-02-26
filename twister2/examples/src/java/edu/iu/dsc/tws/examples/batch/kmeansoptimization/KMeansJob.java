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
package edu.iu.dsc.tws.examples.batch.kmeansoptimization;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.dataobjects.DataFileReadSource;
import edu.iu.dsc.tws.api.dataobjects.DataObjectSink;
import edu.iu.dsc.tws.api.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.data.fs.Path;
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

    KMeansJobParameters kMeansJobParameters = KMeansJobParameters.build(config);

    int parallelismValue = kMeansJobParameters.getParallelismValue();
    int dimension = kMeansJobParameters.getDimension();
    int numFiles = kMeansJobParameters.getNumFiles();
    int dsize = kMeansJobParameters.getDsize();
    int csize = kMeansJobParameters.getCsize();
    int iterations = kMeansJobParameters.getIterations();

    String dinputDirectory = kMeansJobParameters.getDatapointDirectory();
    String cinputDirectory = kMeansJobParameters.getCentroidDirectory();

    double[][] datapoint;
    double[][] centroid;

    if (workerId == 0) {
      try {
        KMeansDataGenerator.generateData(
            "txt", new Path(dinputDirectory), numFiles, dsize, 100, dimension, config);
        KMeansDataGenerator.generateData(
            "txt", new Path(cinputDirectory), numFiles, csize, 100, dimension, config);
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to create input data:", ioe);
      }
    }

    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);

    /** First Graph to partition and read the partitioned data points **/
    DataObjectSource sourceTask = new DataObjectSource();
    DataObjectSink sinkTask = new DataObjectSink();
    taskGraphBuilder.addSource("source", sourceTask, parallelismValue);
    ComputeConnection computeConnection1 = taskGraphBuilder.addSink("sink", sinkTask,
        parallelismValue);
    computeConnection1.direct("source", "direct", DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph dataFlowTaskGraph1 = taskGraphBuilder.build();
    ExecutionPlan plan1 = taskExecutor.plan(dataFlowTaskGraph1);
    taskExecutor.execute(dataFlowTaskGraph1, plan1);

    DataObject<double[][]> dataSet1 = taskExecutor.getOutput(dataFlowTaskGraph1, plan1, "sink");
    DataPartition<double[][]> values1 = dataSet1.getPartitions()[0];
    datapoint = values1.getConsumer().next();
    LOG.info("Partitioned Values Are:%%%%" + Arrays.deepToString(datapoint));

    /** Second Graph to read the centroids **/
    DataFileReadSource task = new DataFileReadSource();
    taskGraphBuilder.addSource("map", task, parallelismValue);
    taskGraphBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph dataFlowTaskGraph2 = taskGraphBuilder.build();
    ExecutionPlan plan2 = taskExecutor.plan(dataFlowTaskGraph2);
    taskExecutor.execute(dataFlowTaskGraph2, plan2);

    DataObject<double[][]> dataSet2 = taskExecutor.getOutput(dataFlowTaskGraph2, plan2, "map");
    DataPartition<double[][]> values2 = dataSet2.getPartitions()[0];
    centroid = values2.getConsumer().next();
    LOG.info("%%% Centroid Values Are:%%%%" + Arrays.deepToString(centroid));

    /** Third Graph to do the distance calculation **/
    KMeansSourceTask kMeansSourceTask = new KMeansSourceTask();
    KMeansAllReduceTask kMeansAllReduceTask = new KMeansAllReduceTask();

    taskGraphBuilder.addSource("source1", kMeansSourceTask, parallelismValue);
    ComputeConnection computeConnection = taskGraphBuilder.addSink("sink1", kMeansAllReduceTask,
        parallelismValue);
    computeConnection.allreduce("source1", "all-reduce", new CentroidAggregator(), DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph graph = taskGraphBuilder.build();

    //Store datapoints and centroids
    DataObject<double[][]> datapoints = new DataObjectImpl<>(config);
    DataObject<double[][]> centroids = new DataObjectImpl<>(config);

    for (int i = 0; i < iterations; i++) {

      datapoints.addPartition(new EntityPartition<>(0, datapoint));
      centroids.addPartition(new EntityPartition<>(0, centroid));

      ExecutionPlan plan = taskExecutor.plan(graph);

      taskExecutor.addInput(graph, plan, "source1", "points", datapoints);
      taskExecutor.addInput(graph, plan, "source1", "centroids", centroids);
      taskExecutor.execute(graph, plan);

      DataObject<double[][]> dataSet = taskExecutor.getOutput(graph, plan, "sink1");
      DataPartition<double[][]> values = dataSet.getPartitions()[0];
      centroid = values.getConsumer().next();
    }
    LOG.info("Centroid Values are:" + Arrays.deepToString(centroid));
  }

  private static class KMeansSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private double[][] centroid = null;
    private double[][] datapoints = null;
    private KMeansCalculator kMeansCalculator = null;

    @Override
    public void execute() {
      LOG.info("Datapoints received for " + context.taskIndex());
      int dim = Integer.parseInt(config.getStringValue("dim"));
      kMeansCalculator = new KMeansCalculator(datapoints, centroid, dim);
      double[][] kMeansCenters = kMeansCalculator.calculate();
      context.writeEnd("all-reduce", kMeansCenters);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void add(String name, DataObject<?> data) {
      LOG.log(Level.INFO, "Received input: " + name);
      if ("points".equals(name)) {
        DataPartition<double[][]> dataPointss = (DataPartition<double[][]>)
            data.getPartitions()[0];
        this.datapoints = dataPointss.getConsumer().next();
      }

      if ("centroids".equals(name)) {
        DataPartition<double[][]> centroidss = (DataPartition<double[][]>)
            data.getPartitions()[0];
        this.centroid = centroidss.getConsumer().next();
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

