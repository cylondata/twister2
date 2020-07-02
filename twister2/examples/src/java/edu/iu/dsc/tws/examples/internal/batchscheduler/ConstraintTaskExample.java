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
package edu.iu.dsc.tws.examples.internal.batchscheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.executor.ExecutorContext;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.IONames;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.data.api.formatters.LocalTextInputPartitioner;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.ComputeEnvironment;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskExecutor;

import mpi.MPI;
import mpi.MPIException;

public class ConstraintTaskExample implements IWorker {

  private static final Logger LOG = Logger.getLogger(ConstraintTaskExample.class.getName());

  public static void main(String[] args) throws ParseException {
    LOG.log(Level.INFO, "Constraint Task Graph Example");

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 1);

    Options options = new Options();
    options.addOption(DataObjectConstants.WORKERS, true, "Workers");
    options.addOption(DataObjectConstants.PARALLELISM_VALUE, true, "parallelism");
    options.addOption(DataObjectConstants.DSIZE, true, "dsize");
    options.addOption(DataObjectConstants.DIMENSIONS, true, "dim");
    options.addOption(Utils.createOption(DataObjectConstants.DINPUT_DIRECTORY,
        true, "Data points Input directory", true));

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    int workers = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.WORKERS));
    int parallelismValue = Integer.parseInt(cmd.getOptionValue(
        DataObjectConstants.PARALLELISM_VALUE));
    int dsize = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.DSIZE));
    int dimension = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.DIMENSIONS));
    String dinput = cmd.getOptionValue(DataObjectConstants.DINPUT_DIRECTORY);

    // build JobConfig
    configurations.put(DataObjectConstants.WORKERS, Integer.toString(workers));
    configurations.put(DataObjectConstants.DSIZE, Integer.toString(dsize));
    configurations.put(DataObjectConstants.DIMENSIONS, Integer.toString(dimension));
    configurations.put(DataObjectConstants.PARALLELISM_VALUE, Integer.toString(parallelismValue));
    configurations.put(DataObjectConstants.DINPUT_DIRECTORY, dinput);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("Constraint-Example");
    jobBuilder.setWorkerClass(ConstraintTaskExample.class.getName());
    jobBuilder.addComputeResource(2, 2048, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }

  @Override
  public void execute(Config config, JobAPI.Job job, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {

    int workerId = workerController.getWorkerInfo().getWorkerID();
    long startTime = System.currentTimeMillis();

    LOG.log(Level.INFO, "Task worker starting: " + job);

    ComputeEnvironment cEnv = ComputeEnvironment.init(config, job, workerController,
        persistentVolume, volatileVolume);
    TaskExecutor taskExecutor = cEnv.getTaskExecutor();

    String dinput = String.valueOf(config.get(DataObjectConstants.DINPUT_DIRECTORY));
    int dimension = Integer.parseInt(String.valueOf(config.get(DataObjectConstants.DIMENSIONS)));
    int parallelismValue
        = Integer.parseInt(String.valueOf(config.get(DataObjectConstants.PARALLELISM_VALUE)));
    int dsize = Integer.parseInt(String.valueOf(config.get(DataObjectConstants.DSIZE)));

    DataGenerator dataGenerator = new DataGenerator(config, workerId);
    dataGenerator.generate(new Path(dinput), dsize, dimension);

    ComputeGraph firstGraph = buildFirstGraph(
        parallelismValue, config, dinput, dsize, dimension, "firstgraphpoints", "1");
    ComputeGraph secondGraph = buildSecondGraph(
        parallelismValue, config, dimension, "firstgraphpoints", "1");

    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(firstGraph);
    taskExecutor.execute(firstGraph, firstGraphExecutionPlan);

    DataObject<Object> firstGraphObject = taskExecutor.getOutput("firstsink");

    //Get the execution plan for the second task graph
    ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(secondGraph);
    taskExecutor.addInput("firstgraphpoints", firstGraphObject);
    taskExecutor.execute(secondGraph, secondGraphExecutionPlan);

    long endTime = System.currentTimeMillis();
    LOG.info("Total Execution Time: " + (endTime - startTime));
  }

  private ComputeGraph buildFirstGraph(int parallelism, Config conf, String dataInput, int dataSize,
                                       int dimension, String inputKey, String constraint) {

    FirstSourceTask sourceTask = new FirstSourceTask(dataInput, dataSize);
    FirstSinkTask sinkTask = new FirstSinkTask(dimension, inputKey);
    ComputeGraphBuilder firstGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
    firstGraphBuilder.addSource("firstsource", sourceTask, parallelism);
    ComputeConnection computeConnection = firstGraphBuilder.addCompute(
        "firstsink", sinkTask, parallelism);

    computeConnection.direct("firstsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    firstGraphBuilder.setMode(OperationMode.BATCH);
    firstGraphBuilder.setTaskGraphName("firstTG");
    firstGraphBuilder.addGraphConstraints(Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER,
        constraint);
    return firstGraphBuilder.build();
  }

  private ComputeGraph buildSecondGraph(int parallelism, Config conf, int dimension,
                                        String inputKey, String constraint) {

    SecondSourceTask sourceTask = new SecondSourceTask(inputKey);
    SecondSinkTask sinkTask = new SecondSinkTask(dimension);
    ComputeGraphBuilder secondGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
    secondGraphBuilder.addSource("secondsource", sourceTask, parallelism);
    ComputeConnection computeConnection = secondGraphBuilder.addCompute(
        "secondsink", sinkTask, parallelism);

    computeConnection.direct("secondsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    secondGraphBuilder.setMode(OperationMode.BATCH);
    secondGraphBuilder.setTaskGraphName("secondTG");
    secondGraphBuilder.addGraphConstraints(
        Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER, constraint);
    return secondGraphBuilder.build();
  }

  private static class FirstSourceTask extends BaseSource {
    private static final long serialVersionUID = -254264120110286748L;

    private DataSource<?, ?> source;
    private String dataDirectory;
    private int dataSize;

    FirstSourceTask(String dataDirectory, int size) {
      setDataDirectory(dataDirectory);
      setDataSize(size);
    }

    public String getDataDirectory() {
      return dataDirectory;
    }

    public void setDataDirectory(String dataDirectory) {
      this.dataDirectory = dataDirectory;
    }

    public int getDataSize() {
      return dataSize;
    }

    public void setDataSize(int dataSize) {
      this.dataSize = dataSize;
    }

    @Override
    public void execute() {
      InputSplit<?> inputSplit = source.getNextSplit(context.taskIndex());
      while (inputSplit != null) {
        try {
          while (!inputSplit.reachedEnd()) {
            Object value = inputSplit.nextRecord(null);
            if (value != null) {
              context.write(Context.TWISTER2_DIRECT_EDGE, value);
            }
          }
          inputSplit = source.getNextSplit(context.taskIndex());
        } catch (IOException e) {
          LOG.log(Level.SEVERE, "Failed to read the input", e);
        }
      }
      context.end(Context.TWISTER2_DIRECT_EDGE);
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
      ExecutionRuntime runtime = (ExecutionRuntime) cfg.get(
          ExecutorContext.TWISTER2_RUNTIME_OBJECT);
      this.source = runtime.createInput(cfg, context, new LocalTextInputPartitioner(
          new Path(getDataDirectory()), context.getParallelism(), cfg));
    }
  }

  private static class FirstSinkTask extends BaseCompute implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    private int length;
    private String inputKey;
    private double[][] dataPointsLocal;

    FirstSinkTask(int len, String inputkey) {
      this.length = len;
      this.inputKey = inputkey;
    }

    @Override
    public boolean execute(IMessage message) {
      List<String> values = new ArrayList<>();
      while (((Iterator) message.getContent()).hasNext()) {
        values.add(String.valueOf(((Iterator) message.getContent()).next()));
      }
      dataPointsLocal = new double[values.size()][length];
      String line;
      for (int i = 0; i < values.size(); i++) {
        line = values.get(i);
        String[] data = line.split(",");
        for (int j = 0; j < length; j++) {
          dataPointsLocal[i][j] = Double.parseDouble(data[j].trim());
        }
      }
      return true;
    }

    @Override
    public DataPartition<double[][]> get(String name) {
      if (!name.equals(inputKey)) {
        throw new RuntimeException("Requesting an unrelated partition " + name);
      }
      return new EntityPartition<>(dataPointsLocal);
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
    }

    @Override
    public IONames getCollectibleNames() {
      return IONames.declare(inputKey);
    }
  }

  private static class SecondSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private DataPartition<?> dataPointsPartition = null;
    private String inputKey;

    SecondSourceTask(String inputkey) {
      this.inputKey = inputkey;
    }

    @Override
    public void execute() {
      double[][] datapoints = (double[][]) dataPointsPartition.getConsumer().next();
      LOG.info("Context Task Index:" + context.taskIndex() + "\t" + datapoints.length);
      context.writeEnd(Context.TWISTER2_DIRECT_EDGE, datapoints);
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(config, context);
    }

    @Override
    public void add(String name, DataPartition<?> data) {
      LOG.log(Level.INFO, "Received input: " + name);
      if (inputKey.equals(name)) {
        this.dataPointsPartition = data;
      }
    }

    @Override
    public IONames getReceivableNames() {
      return IONames.declare(inputKey);
    }
  }

  private static class SecondSinkTask extends BaseCompute {
    private static final long serialVersionUID = -254264120110286748L;

    private static int worldRank = 0;
    private static int worldSize = 0;

    private int length;
    private double[][] dataPointsLocal;

    SecondSinkTask(int len) {
      this.length = len;
    }

    @Override
    public boolean execute(IMessage message) {
      LOG.info("Received message:" + message.getContent().toString());
      List<String> values = new ArrayList<>();
      while (((Iterator) message.getContent()).hasNext()) {
        values.add(String.valueOf(((Iterator) message.getContent()).next()));
      }
      dataPointsLocal = new double[values.size()][length];
      String line;
      for (int i = 0; i < values.size(); i++) {
        line = values.get(i);
        String[] data = line.split(",");
        for (int j = 0; j < length; j++) {
          dataPointsLocal[i][j] = Double.parseDouble(data[j].trim());
        }
      }

      //TODO: SEND THE RECEIVED DATAinputKey FOR THE COMPUTATION
      try {
        worldRank = MPI.COMM_WORLD.getRank();
        worldSize = MPI.COMM_WORLD.getSize();
        int[] res = {1, 2, 3, 4, 5, 6, 7, 8};
        int[] globalSum = new int[res.length];
        MPI.COMM_WORLD.reduce(res, globalSum, res.length, MPI.INT, MPI.SUM, 0);
      } catch (MPIException e) {
        e.printStackTrace();
      }
      return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(config, context);
    }
  }
}
