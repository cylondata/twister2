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
package edu.iu.dsc.tws.examples.batch.cdfw;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.IFunction;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.BaseSink;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansCalculator;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansDataGenerator;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansDataObjectCompute;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansDataObjectDirectSink;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.cdfw.BaseDriver;
import edu.iu.dsc.tws.task.cdfw.CDFWEnv;
import edu.iu.dsc.tws.task.cdfw.DafaFlowJobConfig;
import edu.iu.dsc.tws.task.cdfw.DataFlowGraph;
import edu.iu.dsc.tws.task.dataobjects.DataFileReplicatedReadSource;
import edu.iu.dsc.tws.task.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.cdfw.CDFWWorker;

public final class ConnectedDataflowExample {
  private static final Logger LOG = Logger.getLogger(ConnectedDataflowExample.class.getName());

  private ConnectedDataflowExample() {
  }

  private static String dataDirectory;
  private static String centroidDirectory;
  private static int parallelism;
  private static int iterations;
  private static int dimension;
  private static int dsize;
  private static int csize;

  public static class CDExampleDriver extends BaseDriver {

    @Override
    public void execute(CDFWEnv cdfwEnv) {
      Config config = cdfwEnv.getConfig();
      DafaFlowJobConfig jobConfig = new DafaFlowJobConfig();

      //Generate the datapoints and centroids
      generateData(config);

      DataFlowGraph job1 = generateFirstJob(config, parallelism, jobConfig);
      DataFlowGraph job2 = generateSecondJob(config, parallelism, jobConfig);
      //DataFlowGraph job3 = generateThirdJob(config, parallelism, jobConfig);

      cdfwEnv.executeDataFlowGraph(job1);
      cdfwEnv.executeDataFlowGraph(job2);

      for (int i = 0; i < iterations; i++) {
        DataFlowGraph job3 = generateThirdJob(config, parallelism, jobConfig);
        job3.setIterationNumber(i);
        LOG.info("job iteration at main:" + job3.getIterationNumber());
        cdfwEnv.executeDataFlowGraph(job3);
      }
    }

    public void generateData(Config config) {
      try {
        KMeansDataGenerator.generateData(
            "txt", new Path(dataDirectory), 1, dsize, 100, dimension, config);
        KMeansDataGenerator.generateData(
            "txt", new Path(centroidDirectory), 1, csize, 100, dimension, config);
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to create input data:", ioe);
      }
    }
  }

  public static void main(String[] args) throws ParseException {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 1);

    Options options = new Options();
    options.addOption(CDFConstants.ARGS_PARALLELISM_VALUE, true, "2");
    options.addOption(CDFConstants.ARGS_WORKERS, true, "2");
    options.addOption(CDFConstants.ARGS_DIMENSIONS, true, "2");

    options.addOption(CDFConstants.ARGS_DSIZE, true, "2");
    options.addOption(CDFConstants.ARGS_CSIZE, true, "2");
    options.addOption(CDFConstants.ARGS_DINPUT, true, "2");
    options.addOption(CDFConstants.ARGS_CINPUT, true, "2");
    options.addOption(CDFConstants.ARGS_ITERATIONS, true, "2");

    @SuppressWarnings("deprecation")
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine = commandLineParser.parse(options, args);

    int instances = Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_WORKERS));
    parallelism =
        Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_PARALLELISM_VALUE));
    dimension =
        Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_DIMENSIONS));

    dsize = Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_DSIZE));
    csize = Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_CSIZE));
    iterations = Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_CSIZE));

    dataDirectory = commandLine.getOptionValue(CDFConstants.ARGS_DINPUT);
    centroidDirectory = commandLine.getOptionValue(CDFConstants.ARGS_CINPUT);

    configurations.put(CDFConstants.ARGS_WORKERS, Integer.toString(instances));
    configurations.put(CDFConstants.ARGS_PARALLELISM_VALUE, Integer.toString(parallelism));
    configurations.put(CDFConstants.ARGS_DIMENSIONS, Integer.toString(dimension));

    configurations.put(CDFConstants.ARGS_CSIZE, Integer.toString(dsize));
    configurations.put(CDFConstants.ARGS_DSIZE, Integer.toString(csize));

    configurations.put(CDFConstants.ARGS_DINPUT, dataDirectory);
    configurations.put(CDFConstants.ARGS_CINPUT, centroidDirectory);
    configurations.put(CDFConstants.ARGS_ITERATIONS, iterations);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    config = Config.newBuilder().putAll(config)
        .put(SchedulerContext.DRIVER_CLASS, null).build();

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(CDExampleDriver.class.getName())
        .setWorkerClass(CDFWWorker.class)
        .setDriverClass(CDExampleDriver.class.getName())
        .addComputeResource(1, 512, instances)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }


  private static DataFlowGraph generateFirstJob(Config config, int parallelismValue,
                                                DafaFlowJobConfig jobConfig) {

    DataObjectSource dataObjectSource = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory);
    KMeansDataObjectCompute dataObjectCompute = new KMeansDataObjectCompute(
        Context.TWISTER2_DIRECT_EDGE, dsize, parallelismValue, dimension);
    KMeansDataObjectDirectSink dataObjectSink = new KMeansDataObjectDirectSink("points");
    ComputeGraphBuilder datapointsComputeGraphBuilder = ComputeGraphBuilder.newBuilder(config);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    datapointsComputeGraphBuilder.addSource("datapointsource", dataObjectSource,
        parallelismValue);
    ComputeConnection datapointComputeConnection = datapointsComputeGraphBuilder.addCompute(
        "datapointcompute", dataObjectCompute, parallelismValue);
    ComputeConnection firstGraphComputeConnection = datapointsComputeGraphBuilder.addSink(
        "datapointsink", dataObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    datapointComputeConnection.direct("datapointsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    firstGraphComputeConnection.direct("datapointcompute")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    datapointsComputeGraphBuilder.setMode(OperationMode.BATCH);

    datapointsComputeGraphBuilder.setTaskGraphName("datapointsTG");
    ComputeGraph firstGraph = datapointsComputeGraphBuilder.build();

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("datapointsink", firstGraph)
        .setWorkers(2).addDataFlowJobConfig(jobConfig)
        .addOutput("points", "datapointsink")
        .setGraphType("non-iterative");
    //.addOutput("first_graph", "first_out", "datapointsink");
    return job;
  }

  private static DataFlowGraph generateSecondJob(Config config, int parallelismValue,
                                                 DafaFlowJobConfig jobConfig) {

    DataFileReplicatedReadSource dataFileReplicatedReadSource
        = new DataFileReplicatedReadSource(Context.TWISTER2_DIRECT_EDGE, centroidDirectory);
    KMeansDataObjectCompute centroidObjectCompute = new KMeansDataObjectCompute(
        Context.TWISTER2_DIRECT_EDGE, csize, dimension);
    KMeansDataObjectDirectSink centroidObjectSink = new KMeansDataObjectDirectSink("centroids");
    ComputeGraphBuilder centroidsComputeGraphBuilder = ComputeGraphBuilder.newBuilder(config);

    //Add source, compute, and sink tasks to the task graph builder for the second task graph
    centroidsComputeGraphBuilder.addSource("centroidsource", dataFileReplicatedReadSource,
        parallelismValue);
    ComputeConnection centroidComputeConnection = centroidsComputeGraphBuilder.addCompute(
        "centroidcompute", centroidObjectCompute, parallelismValue);
    ComputeConnection secondGraphComputeConnection = centroidsComputeGraphBuilder.addSink(
        "centroidsink", centroidObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    centroidComputeConnection.direct("centroidsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    secondGraphComputeConnection.direct("centroidcompute")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    centroidsComputeGraphBuilder.setMode(OperationMode.BATCH);
    centroidsComputeGraphBuilder.setTaskGraphName("centTG");

    //Build the second taskgraph
    ComputeGraph secondGraph = centroidsComputeGraphBuilder.build();
    DataFlowGraph job = DataFlowGraph.newSubGraphJob("centroidsink", secondGraph)
        .setWorkers(2).addDataFlowJobConfig(jobConfig)
        .addOutput("centroids", "centroidsink")
        .setGraphType("non-iterative");
    //.addOutput("second_graph", "second_out", "centroidsink");
    return job;
  }


  private static DataFlowGraph generateThirdJob(Config config, int parallelismValue,
                                                DafaFlowJobConfig jobConfig) {

    KMeansSourceTask kMeansSourceTask = new KMeansSourceTask();
    KMeansAllReduceTask kMeansAllReduceTask = new KMeansAllReduceTask();
    ComputeGraphBuilder kmeansComputeGraphBuilder = ComputeGraphBuilder.newBuilder(config);

    //Add source, and sink tasks to the task graph builder for the third task graph
    kmeansComputeGraphBuilder.addSource("kmeanssource", kMeansSourceTask, parallelismValue);
    ComputeConnection kMeanscomputeConnection = kmeansComputeGraphBuilder.addSink(
        "kmeanssink", kMeansAllReduceTask, 2);

    //Creating the communication edges between the tasks for the third task graph
    kMeanscomputeConnection.allreduce("kmeanssource")
        .viaEdge("all-reduce")
        .withReductionFunction(new CentroidAggregator())
        .withDataType(MessageTypes.OBJECT);
    kmeansComputeGraphBuilder.setMode(OperationMode.BATCH);
    kmeansComputeGraphBuilder.setTaskGraphName("kmeansTG");
    ComputeGraph thirdGraph = kmeansComputeGraphBuilder.build();

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("kmeansTG", thirdGraph)
        .setWorkers(2).addDataFlowJobConfig(jobConfig)
        .addInput("datapointsink", "points", "datapointsink")
        .addInput("centroidsink", "centroids", "centroidsink")
        .setGraphType("iterative")
        .setIterations(100);
    return job;
  }

  public static class KMeansSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private double[][] centroid = null;
    private double[][] datapoints = null;

    private KMeansCalculator kMeansCalculator = null;
    private DataObject<?> dataPointsObject = null;
    private DataObject<?> centroidsObject = null;

    public KMeansSourceTask() {
    }

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
      LOG.info("New centers:" + Arrays.deepToString(kMeansCenters));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void add(String name, DataObject<?> data) {
      LOG.info("Received input: " + name  + "\t" + data);
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

    public KMeansAllReduceTask() {
    }

    @Override
    public boolean execute(IMessage message) {
      //LOG.info("Received centroids: " + context.getWorkerId()
      //    + ":" + context.globalTaskId() + "\t" + message.getContent());
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
      Set<String> inputKeys = new HashSet<>();
      inputKeys.add("centroids");
      return inputKeys;
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

    public CentroidAggregator() {
    }

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
      //LOG.info("New Centroid Value:" + Arrays.deepToString(newCentroids));
      return newCentroids;
    }
  }
}

