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

import java.util.HashMap;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.IFunction;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.IONames;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansCalculator;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansDataObjectCompute;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansDataObjectDirectSink;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.cdfw.BaseDriver;
import edu.iu.dsc.tws.task.cdfw.CDFWEnv;
import edu.iu.dsc.tws.task.cdfw.DataFlowGraph;
import edu.iu.dsc.tws.task.cdfw.DataFlowJobConfig;
import edu.iu.dsc.tws.task.dataobjects.DataFileReplicatedReadSource;
import edu.iu.dsc.tws.task.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.cdfw.CDFWWorker;

public final class KMeansConnectedDataflowExample {
  private static final Logger LOG
      = Logger.getLogger(KMeansConnectedDataflowExample.class.getName());

  private KMeansConnectedDataflowExample() {
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

    String dataDirectory = commandLine.getOptionValue(CDFConstants.ARGS_DINPUT);
    String centroidDirectory = commandLine.getOptionValue(CDFConstants.ARGS_CINPUT);
    int instances = Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_WORKERS));
    int parallelism =
        Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_PARALLELISM_VALUE));
    int dimension =
        Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_DIMENSIONS));
    int dsize =
        Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_DSIZE));
    int csize =
        Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_CSIZE));
    int iterations =
        Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_ITERATIONS));

    config = Config.newBuilder().putAll(config)
        .put(CDFConstants.ARGS_WORKERS, Integer.toString(instances))
        .put(CDFConstants.ARGS_PARALLELISM_VALUE, Integer.toString(parallelism))
        .put(CDFConstants.ARGS_DIMENSIONS, Integer.toString(dimension))
        .put(CDFConstants.ARGS_CSIZE, Integer.toString(csize))
        .put(CDFConstants.ARGS_DSIZE, Integer.toString(dsize))
        .put(CDFConstants.ARGS_DINPUT, dataDirectory)
        .put(CDFConstants.ARGS_CINPUT, centroidDirectory)
        .put(CDFConstants.ARGS_ITERATIONS, Integer.toString(iterations))
        .put(SchedulerContext.DRIVER_CLASS, null).build();

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName("kmeans-connected-dataflow")
        .setWorkerClass(CDFWWorker.class)
        .setDriverClass(KMeansDriver.class.getName())
        .addComputeResource(1, 2048, instances, true)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }

  private static DataFlowGraph generateFirstJob(Config config, int parallelismValue,
                                                String dataDirectory, int dimension,
                                                int dsize, int instances,
                                                DataFlowJobConfig jobConfig) {

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
    ComputeConnection firstGraphComputeConnection = datapointsComputeGraphBuilder.addCompute(
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

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("dsink", firstGraph)
        .setWorkers(instances).addDataFlowJobConfig(jobConfig)
        .setGraphType("non-iterative");
    return job;
  }

  private static DataFlowGraph generateSecondJob(Config config, int parallelismValue,
                                                 String centroidDirectory, int dimension,
                                                 int csize, int instances,
                                                 DataFlowJobConfig jobConfig) {

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
    ComputeConnection secondGraphComputeConnection = centroidsComputeGraphBuilder.addCompute(
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
    DataFlowGraph job = DataFlowGraph.newSubGraphJob("csink", secondGraph)
        .setWorkers(instances).addDataFlowJobConfig(jobConfig)
        .setGraphType("non-iterative");
    return job;
  }

  private static DataFlowGraph generateThirdJob(Config config, int parallelismValue,
                                                int instances, int iterations,
                                                int dimension, DataFlowJobConfig jobConfig) {

    KMeansSourceTask kMeansSourceTask = new KMeansSourceTask(dimension);
    KMeansAllReduceTask kMeansAllReduceTask = new KMeansAllReduceTask();
    ComputeGraphBuilder kmeansComputeGraphBuilder = ComputeGraphBuilder.newBuilder(config);

    //Add source, and sink tasks to the task graph builder for the third task graph
    kmeansComputeGraphBuilder.addSource("kmeanssource", kMeansSourceTask, parallelismValue);
    ComputeConnection kMeanscomputeConnection = kmeansComputeGraphBuilder.addCompute(
        "kmeanssink", kMeansAllReduceTask, parallelismValue);

    //Creating the communication edges between the tasks for the third task graph
    kMeanscomputeConnection.allreduce("kmeanssource")
        .viaEdge("all-reduce")
        .withReductionFunction(new CentroidAggregator())
        .withDataType(MessageTypes.OBJECT);
    kmeansComputeGraphBuilder.setMode(OperationMode.BATCH);
    kmeansComputeGraphBuilder.setTaskGraphName("kmeansTG");
    ComputeGraph thirdGraph = kmeansComputeGraphBuilder.build();

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("kmeansTG", thirdGraph)
        .setWorkers(instances).addDataFlowJobConfig(jobConfig)
        .setGraphType("iterative")
        .setIterations(iterations);
    return job;
  }

  public static class KMeansDriver extends BaseDriver {

    @Override
    public void execute(CDFWEnv cdfwEnv) {
      Config config = cdfwEnv.getConfig();
      DataFlowJobConfig jobConfig = new DataFlowJobConfig();

      String dataDirectory = String.valueOf(config.get(CDFConstants.ARGS_DINPUT));
      String centroidDirectory = String.valueOf(config.get(CDFConstants.ARGS_CINPUT));
      int parallelism =
          Integer.parseInt(String.valueOf(config.get(CDFConstants.ARGS_PARALLELISM_VALUE)));
      int instances = Integer.parseInt(String.valueOf(config.get(CDFConstants.ARGS_WORKERS)));
      int iterations =
          Integer.parseInt(String.valueOf(config.get(CDFConstants.ARGS_ITERATIONS)));
      int dimension = Integer.parseInt(String.valueOf(config.get(CDFConstants.ARGS_DIMENSIONS)));
      int dsize = Integer.parseInt(String.valueOf(config.get(CDFConstants.ARGS_DSIZE)));
      int csize = Integer.parseInt(String.valueOf(config.get(CDFConstants.ARGS_CSIZE)));

      //generateData(config, dataDirectory, centroidDirectory, dimension, dsize, csize);

      DataFlowGraph job = generateData(config, dataDirectory, centroidDirectory, dimension,
          dsize, csize, instances, parallelism, jobConfig);

      DataFlowGraph job1 = generateFirstJob(config, parallelism, dataDirectory, dimension,
          dsize, instances, jobConfig);
      DataFlowGraph job2 = generateSecondJob(config, parallelism, centroidDirectory, dimension,
          csize, instances, jobConfig);

      cdfwEnv.executeDataFlowGraph(job);
      cdfwEnv.executeDataFlowGraph(job1);
      cdfwEnv.executeDataFlowGraph(job2);

      for (int i = 0; i < iterations; i++) {
        DataFlowGraph job3 = generateThirdJob(config, parallelism, instances, iterations,
            dimension, jobConfig);
        job3.setIterationNumber(i);
        cdfwEnv.executeDataFlowGraph(job3);
      }

      //Kubernetes scale up
      /*if (cdfwEnv.increaseWorkers(instances)) {
        for (int i = 0; i < iterations; i++) {
          DataFlowGraph job3 = generateThirdJob(config, 4, instances, iterations,
              dimension, jobConfig);
          job3.setIterationNumber(i);
          cdfwEnv.executeDataFlowGraph(job3);
        }
      }*/
      cdfwEnv.close();
    }

    /*public void generateData(Config config, String dataDirectory, String centroidDirectory,
                             int dimension, int dsize, int csize) {
      try {
        int numOfFiles = 1;
        int sizeMargin = 100;
        KMeansDataGenerator.generateData("txt", new Path(dataDirectory), numOfFiles, dsize,
            sizeMargin, dimension, config);
        KMeansDataGenerator.generateData("txt", new Path(centroidDirectory), numOfFiles, csize,
            sizeMargin, dimension, config);
      } catch (IOException ioe) {
        throw new Twister2RuntimeException("Failed to create input data:", ioe);
      }
    }*/
  }

  private static DataFlowGraph generateData(Config config, String dataDirectory,
                                            String centroidDirectory, int dimension, int dsize,
                                            int csize, int workers, int parallel,
                                            DataFlowJobConfig jobConfig) {

    DataGeneratorSource dataGeneratorSource = new DataGeneratorSource(Context.TWISTER2_DIRECT_EDGE,
        dsize, csize, dimension, dataDirectory, centroidDirectory);
    DataGeneratorSink dataGeneratorSink = new DataGeneratorSink();
    ComputeGraphBuilder dataGenerationGraphBuilder = ComputeGraphBuilder.newBuilder(config);
    dataGenerationGraphBuilder.setTaskGraphName("DataGenerator");
    dataGenerationGraphBuilder.addSource("datageneratorsource", dataGeneratorSource, parallel);

    ComputeConnection dataObjectComputeConnection = dataGenerationGraphBuilder.addCompute(
        "datageneratorsink", dataGeneratorSink, parallel);
    dataObjectComputeConnection.direct("datageneratorsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    dataGenerationGraphBuilder.setMode(OperationMode.BATCH);
    ComputeGraph dataObjectTaskGraph = dataGenerationGraphBuilder.build();
    dataGenerationGraphBuilder.setTaskGraphName("datageneratorTG");

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("datageneratorsink", dataObjectTaskGraph)
        .setWorkers(workers).addDataFlowJobConfig(jobConfig)
        .setGraphType("non-iterative");
    return job;
  }


  public static class KMeansSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private double[][] centroid = null;
    private double[][] datapoints = null;

    private KMeansCalculator kMeansCalculator = null;

    private DataPartition<?> dataPartition = null;
    private DataPartition<?> centroidPartition = null;

    private int dimension = 0;

    public KMeansSourceTask() {
    }

    public KMeansSourceTask(int dim) {
      this.dimension = dim;
    }

    @Override
    public void execute() {
      datapoints = (double[][]) dataPartition.first();
      centroid = (double[][]) centroidPartition.first();
      kMeansCalculator = new KMeansCalculator(datapoints, centroid, dimension);
      double[][] kMeansCenters = kMeansCalculator.calculate();
      context.writeEnd("all-reduce", kMeansCenters);
    }

    @Override
    public void add(String name, DataObject<?> data) {
    }

    @Override
    public void add(String name, DataPartition<?> data) {
      if ("points".equals(name)) {
        this.dataPartition = data;
      }
      if ("centroids".equals(name)) {
        this.centroidPartition = data;
      }
    }

    @Override
    public IONames getReceivableNames() {
      return IONames.declare("points", "centroids");
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
    }
  }

  public static class KMeansAllReduceTask extends BaseCompute implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    private double[][] centroids;
    private double[][] newCentroids;

    public KMeansAllReduceTask() {
    }

    @Override
    public boolean execute(IMessage message) {
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
      return new EntityPartition<>(newCentroids);
    }

    @Override
    public IONames getCollectibleNames() {
      return IONames.declare("centroids");
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
      return newCentroids;
    }
  }
}

