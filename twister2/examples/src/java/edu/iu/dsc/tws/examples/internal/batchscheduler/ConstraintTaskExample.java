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
import java.util.Random;
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
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.BaseSink;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.data.api.formatters.LocalTextInputPartitioner;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.ComputeEnvironment;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskExecutor;

public class ConstraintTaskExample implements IWorker {

  private static final Logger LOG = Logger.getLogger(ConstraintTaskExample.class.getName());

 /* private int workers;
  private int parallelismValue;
  private int dsize;
  private int dimension;
  private String dinput;*/

  @Override
  public void execute(Config config, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {

    long startTime = System.currentTimeMillis();

    LOG.log(Level.INFO, "Task worker starting: " + workerID);

    ComputeEnvironment cEnv = ComputeEnvironment.init(config, workerID, workerController,
        persistentVolume, volatileVolume);
    TaskExecutor taskExecutor = cEnv.getTaskExecutor();

    String dinput = String.valueOf(config.get(DataObjectConstants.DINPUT_DIRECTORY));
    int dimension = Integer.parseInt(String.valueOf(config.get(DataObjectConstants.DIMENSIONS)));
    int parallelismValue
        = Integer.parseInt(String.valueOf(config.get(DataObjectConstants.PARALLELISM_VALUE)));
    int dsize = Integer.parseInt(String.valueOf(config.get(DataObjectConstants.DSIZE)));

    LOG.info("Data Input Directory:" + dinput);
    DataGenerator dataGenerator = new DataGenerator(config, workerID);
    dataGenerator.generate(new Path(dinput), dsize, dimension);

    ComputeGraph firstGraph = buildFirstGraph(parallelismValue, config, dinput, dsize);
    ComputeGraph secondGraph = buildSecondGraph(parallelismValue, config, dinput, dsize);

    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(firstGraph);
    taskExecutor.execute(firstGraph, firstGraphExecutionPlan);
    DataObject<Object> firstGraphObject = taskExecutor.getOutput(
        firstGraph, firstGraphExecutionPlan, "firstsink");

    //Get the execution plan for the second task graph
    ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(secondGraph);
    taskExecutor.addInput(secondGraph, secondGraphExecutionPlan,
        "secondsource", "secondgraphpoints", firstGraphObject);
    /*taskExecutor.execute(secondGraph, secondGraphExecutionPlan);
    DataObject<Object> secondGraphObject = taskExecutor.getOutput(
        secondGraph, secondGraphExecutionPlan, "secondsink");
    LOG.info("Second Graph Object:" + secondGraphObject);*/
    long endTime = System.currentTimeMillis();
    LOG.info("Total Execution Time: " + (endTime - startTime));
  }

  private ComputeGraph buildFirstGraph(int parallelism, Config conf,
                                       String dataInput, int dataSize) {
    FirstSourceTask sourceTask = new FirstSourceTask(dataInput, dataSize);
    FirstSinkTask sinkTask = new FirstSinkTask(dataSize);

    ComputeGraphBuilder firstGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
    firstGraphBuilder.addSource("firstsource", sourceTask, parallelism);
    ComputeConnection computeConnection = firstGraphBuilder.addSink(
        "firstsink", sinkTask, parallelism);

    computeConnection.direct("firstsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    firstGraphBuilder.setMode(OperationMode.BATCH);
    firstGraphBuilder.setTaskGraphName("firstTG");
    return firstGraphBuilder.build();
  }

  private ComputeGraph buildSecondGraph(int parallelism, Config conf,
                                        String dataInput, int dataSize) {
    SecondSourceTask sourceTask = new SecondSourceTask(dataInput, dataSize);
    SecondSinkTask sinkTask = new SecondSinkTask();

    ComputeGraphBuilder secondGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
    secondGraphBuilder.addSource("secondsource", sourceTask, parallelism);
    ComputeConnection computeConnection = secondGraphBuilder.addSink(
        "secondsink", sinkTask, parallelism);

    computeConnection.direct("secondsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    secondGraphBuilder.setMode(OperationMode.BATCH);
    secondGraphBuilder.setTaskGraphName("secondTG");
    return secondGraphBuilder.build();
  }

  private static class FirstSourceTask extends BaseSource {
    private static final long serialVersionUID = -254264120110286748L;

    private double[] datapoints = null;
    private int numPoints = 0;
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

  private static class FirstSinkTask extends BaseSink implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    private double[] datapoints;
    private int length;
    private double[][] dataPointsLocal;

    FirstSinkTask(int len) {
      this.length = len;
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
        context.write(Context.TWISTER2_DIRECT_EDGE, dataPointsLocal);
      }
      /*List<double[]> values = new ArrayList<>();
      while (((Iterator) message.getContent()).hasNext()) {
        values.add((double[]) ((Iterator) message.getContent()).next());
      }
      datapoints = new double[values.size()];
      for (double[] value : values) {
        datapoints = value;
      }*/
      return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
    }

    @Override
    public DataPartition<double[]> get() {
      return new EntityPartition<>(context.taskIndex(), datapoints);
    }
  }

  private static class SecondSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private double[] datapoints = null;
    private int numPoints = 0;
    private DataObject<?> dataPointsObject = null;

    private String dataDirectory;
    private int dataSize;

    SecondSourceTask(String dataDirectory, int size) {
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
      int seedValue = 100;
      datapoints = new double[numPoints];
      Random r = new Random(seedValue);
      for (int i = 0; i < numPoints; i++) {
        double randomValue = r.nextDouble();
        datapoints[i] = randomValue;
      }
      context.writeEnd(Context.TWISTER2_DIRECT_EDGE, datapoints);
    }

    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
      numPoints = Integer.parseInt(cfg.getStringValue(DataObjectConstants.DSIZE));
    }

    @Override
    public void add(String name, DataObject<?> data) {
      if ("points".equals(name)) {
        this.dataPointsObject = data;
      }
    }
  }

  private static class SecondSinkTask extends BaseSink {
    private static final long serialVersionUID = -5190777711234234L;

    private double[] datapoints;

    SecondSinkTask() {
    }

    @Override
    public boolean execute(IMessage message) {
      List<double[]> values = new ArrayList<>();
      while (((Iterator) message.getContent()).hasNext()) {
        values.add((double[]) ((Iterator) message.getContent()).next());
      }
      datapoints = new double[values.size()];
      for (double[] value : values) {
        datapoints = value;
      }
      return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
    }
  }

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

    @SuppressWarnings("deprecation")
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
}
