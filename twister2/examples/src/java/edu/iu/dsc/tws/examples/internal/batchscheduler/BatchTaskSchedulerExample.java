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

import java.util.ArrayList;
import java.util.Arrays;
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
import edu.iu.dsc.tws.api.compute.IFunction;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
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
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.ComputeEnvironment;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskExecutor;

public class BatchTaskSchedulerExample implements IWorker {

  private static final Logger LOG = Logger.getLogger(BatchTaskSchedulerExample.class.getName());

  private static ComputeGraph buildFirstGraph(int parallelism, Config conf) {

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    FirstSourceTask sourceTask = new FirstSourceTask();
    FirstComputeTask computeTask = new FirstComputeTask();
    FirstSinkTask sinkTask = new FirstSinkTask("firstgraphpoints");

    ComputeGraphBuilder firstGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
    firstGraphBuilder.addSource("firstsource", sourceTask, parallelism);
    ComputeConnection computeConnection = firstGraphBuilder.addCompute(
        "firstcompute", computeTask, parallelism);
    ComputeConnection sinkConnection = firstGraphBuilder.addCompute(
        "firstsink", sinkTask, parallelism);

    //Creating the communication edges between the tasks for the second task graph
    computeConnection.direct("firstsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    sinkConnection.direct("firstcompute")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    firstGraphBuilder.setMode(OperationMode.BATCH);
    firstGraphBuilder.setTaskGraphName("firstTG");
    return firstGraphBuilder.build();
  }

  private static ComputeGraph buildSecondGraph(int parallelism, Config conf) {

    //Add source, compute, and sink tasks to the task graph builder for the third task graph
    SecondSourceTask sourceTask = new SecondSourceTask();
    SecondComputeTask computeTask = new SecondComputeTask();
    SecondSinkTask sinkTask = new SecondSinkTask("secondgraphpoints");

    ComputeGraphBuilder secondGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
    secondGraphBuilder.addSource("secondsource", sourceTask, parallelism);
    ComputeConnection computeConnection = secondGraphBuilder.addCompute(
        "secondcompute", computeTask, parallelism);
    ComputeConnection sinkConnection = secondGraphBuilder.addCompute(
        "secondsink", sinkTask, parallelism);

    //Creating the communication edges between the tasks for the second task graph
    computeConnection.direct("secondsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    sinkConnection.direct("secondcompute")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    secondGraphBuilder.setMode(OperationMode.BATCH);
    secondGraphBuilder.setTaskGraphName("secondTG");
    return secondGraphBuilder.build();
  }

  private static ComputeGraph buildThirdGraph(int parallelism, Config conf) {

    //Add source, compute, and sink tasks to the task graph builder for the third task graph
    ThirdSourceTask sourceTask = new ThirdSourceTask();
    ThirdSinkTask sinkTask = new ThirdSinkTask("thirdgraphpoints");

    ComputeGraphBuilder thirdGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
    thirdGraphBuilder.addSource("thirdsource", sourceTask, parallelism);
    ComputeConnection sinkConnection = thirdGraphBuilder.addCompute(
        "thirdsink", sinkTask, parallelism);

    //Creating the communication edges between the tasks for the third task graph
    sinkConnection.allreduce("thirdsource")
        .viaEdge("all-reduce")
        .withReductionFunction(new Aggregator())
        .withDataType(MessageTypes.OBJECT);
    thirdGraphBuilder.setMode(OperationMode.BATCH);
    thirdGraphBuilder.setTaskGraphName("thirdTG");
    return thirdGraphBuilder.build();
  }

  @Override
  public void execute(Config config, JobAPI.Job job, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {

    int workerId = workerController.getWorkerInfo().getWorkerID();
    long startTime = System.currentTimeMillis();

    LOG.log(Level.FINE, "Task worker starting: " + job);

    ComputeEnvironment cEnv = ComputeEnvironment.init(config, job, workerController,
        persistentVolume, volatileVolume);
    TaskExecutor taskExecutor = cEnv.getTaskExecutor();

    //Independent Graph and it has collector
    ComputeGraph firstGraph = buildFirstGraph(2, config);

    //Dependent Graph and it has collector
    ComputeGraph secondGraph = buildSecondGraph(4, config);

    //Dependent Graph and it has receptor to receive the input from second graph or first graph
    ComputeGraph thirdGraph = buildThirdGraph(4, config);

    ComputeGraph[] computeGraphs = new ComputeGraph[] {firstGraph, secondGraph, thirdGraph};

    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(firstGraph);

    //Get the execution plan for the second task graph
    ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(secondGraph);

    //Get the execution plan for the third task graph
    ExecutionPlan thirdGraphExecutionPlan = taskExecutor.plan(thirdGraph);

    taskExecutor.execute(firstGraph, firstGraphExecutionPlan);
    taskExecutor.execute(secondGraph, secondGraphExecutionPlan);
    taskExecutor.execute(thirdGraph, thirdGraphExecutionPlan);

    //This is to test all the three graphs as dependent
    /*Map<String, ExecutionPlan> taskExecutionPlan = taskExecutor.plan(computeGraphs);
    for (Map.Entry<String, ExecutionPlan> planEntry : taskExecutionPlan.entrySet()) {
      String graphName = planEntry.getKey();
      if (graphName.equals(computeGraphs[0].getGraphName())) {
        taskExecutor.execute(computeGraphs[0], planEntry.getValue());
      } else if (graphName.equals(computeGraphs[1].getGraphName())) {
        taskExecutor.execute(computeGraphs[1], planEntry.getValue());
      } else {
        taskExecutor.execute(computeGraphs[2], planEntry.getValue());
      }
    }*/
    cEnv.close();
    long endTime = System.currentTimeMillis();
    LOG.info("Total Execution Time: " + (endTime - startTime));
  }

  private static class FirstSourceTask extends BaseSource {
    private static final long serialVersionUID = -254264120110286748L;

    private double[] datapoints = null;
    private int numPoints = 0;

    FirstSourceTask() {
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
  }

  private static class FirstComputeTask extends BaseCompute {
    private static final long serialVersionUID = -254264120110286748L;

    FirstComputeTask() {
    }

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.FINE, "Received Points: " + context.getWorkerId()
          + ":" + context.globalTaskId());
      if (message.getContent() instanceof Iterator) {
        Iterator it = (Iterator) message.getContent();
        while (it.hasNext()) {
          context.write(Context.TWISTER2_DIRECT_EDGE, it.next());
        }
      }
      context.end(Context.TWISTER2_DIRECT_EDGE);
      return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
    }
  }

  private static class FirstSinkTask extends BaseCompute implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    private double[] datapoints;
    private String inputKey;

    FirstSinkTask(String inputkey) {
      this.inputKey = inputkey;
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

    @Override
    public DataPartition<double[]> get() {
      return new EntityPartition<>(datapoints);
    }

    @Override
    public IONames getCollectibleNames() {
      return IONames.declare(inputKey);
    }
  }

  private static class SecondSourceTask extends BaseSource {
    private static final long serialVersionUID = -254264120110286748L;

    private double[] datapoints = null;
    private int numPoints = 0;

    SecondSourceTask() {
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
  }

  private static class SecondComputeTask extends BaseCompute {
    private static final long serialVersionUID = -254264120110286748L;

    SecondComputeTask() {
    }

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.FINE, "Received Points: " + context.getWorkerId()
          + ":" + context.globalTaskId());
      if (message.getContent() instanceof Iterator) {
        Iterator it = (Iterator) message.getContent();
        while (it.hasNext()) {
          context.write(Context.TWISTER2_DIRECT_EDGE, it.next());
        }
      }
      context.end(Context.TWISTER2_DIRECT_EDGE);
      return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
    }
  }


  private static class SecondSinkTask extends BaseCompute implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    private double[] datapoints;
    private String inputKey;

    SecondSinkTask(String inputkey) {
      this.inputKey = inputkey;
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

    @Override
    public DataPartition<double[]> get() {
      return new EntityPartition<>(datapoints);
    }

    @Override
    public IONames getCollectibleNames() {
      return IONames.declare(inputKey);
    }
  }

  private static class ThirdSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private double[] datapoints = null;
    private DataPartition<?> dataPartition = null;

    ThirdSourceTask() {
    }

    @Override
    public void execute() {
      datapoints = (double[]) dataPartition.first();
      context.writeEnd("all-reduce", datapoints);
    }

    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
    }

    @Override
    public void add(String name, DataObject<?> data) {
    }

    @Override
    public void add(String name, DataPartition<?> data) {
      if ("secondgraphpoints".equals(name)) {
        this.dataPartition = data;
      }
    }

    @Override
    public IONames getReceivableNames() {
      //add the "firstgraphpoints" here if you want to connect the first graph with third graph
      return IONames.declare("secondgraphpoints");
    }
  }

  private static class ThirdSinkTask extends BaseCompute implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    private double[] datapoints;
    private String inputKey;

    ThirdSinkTask() {
    }

    ThirdSinkTask(String inputkey) {
      this.inputKey = inputkey;
    }

    @Override
    public boolean execute(IMessage message) {
      datapoints = (double[]) message.getContent();
      return true;
    }

    @Override
    public DataPartition<double[]> get() {
      return new EntityPartition<>(datapoints);
    }

    @Override
    public IONames getCollectibleNames() {
      return IONames.declare(inputKey);
    }
  }

  private static class Aggregator implements IFunction {
    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public Object onMessage(Object object1, Object object2) throws ArrayIndexOutOfBoundsException {

      double[] object11 = (double[]) object1;
      double[] object21 = (double[]) object2;

      LOG.fine("object 1 and object 2:"
          + Arrays.toString(object11) + "\t\n" + Arrays.toString(object21));

      double[] object31 = new double[object11.length];

      for (int j = 0; j < object11.length; j++) {
        double newVal = object11[j] + object21[j];
        object31[j] = newVal;
      }

      LOG.fine("Object 3 len:" + object31.length
          + "\tObject 3:" + Arrays.toString(object31));
      return object31;
    }
  }

  public static void main(String[] args) throws ParseException {
    LOG.log(Level.INFO, "Batch Task Graph Example");

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    Options options = new Options();
    options.addOption(DataObjectConstants.WORKERS, true, "Workers");
    options.addOption(DataObjectConstants.PARALLELISM_VALUE, true, "parallelism");
    options.addOption(DataObjectConstants.DSIZE, true, "dsize");

    @SuppressWarnings("deprecation")
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    int workers = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.WORKERS));
    int parallelismValue = Integer.parseInt(cmd.getOptionValue(
        DataObjectConstants.PARALLELISM_VALUE));
    int dsize = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.DSIZE));

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put(DataObjectConstants.WORKERS, Integer.toString(workers));
    jobConfig.put(DataObjectConstants.PARALLELISM_VALUE, Integer.toString(parallelismValue));
    jobConfig.put(DataObjectConstants.DSIZE, Integer.toString(dsize));

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("BatchScheduler-test");
    jobBuilder.setWorkerClass(BatchTaskSchedulerExample.class.getName());
    jobBuilder.addComputeResource(2, 2048, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
