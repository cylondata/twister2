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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
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
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.compute.nodes.BaseSink;
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
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.ComputeEnvironment;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskExecutor;

public class BatchTaskSchedulerExample implements IWorker {

  private static final Logger LOG = Logger.getLogger(BatchTaskSchedulerExample.class.getName());

  private static ComputeGraph buildFirstGraph(int parallelism,
                                              Config conf) {

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    FirstSourceTask sourceTask = new FirstSourceTask();
    FirstComputeTask computeTask = new FirstComputeTask();
    FirstSinkTask sinkTask = new FirstSinkTask("firstgraphpoints");

    ComputeGraphBuilder firstGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
    firstGraphBuilder.addSource("firstsource", sourceTask, parallelism);
    ComputeConnection computeConnection = firstGraphBuilder.addCompute(
        "firstcompute", computeTask, parallelism);
    ComputeConnection sinkConnection = firstGraphBuilder.addSink(
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

  private static ComputeGraph buildSecondGraph(int parallelism,
                                               Config conf) {
    //Add source, compute, and sink tasks to the task graph builder for the second task graph
    SecondSourceTask sourceTask = new SecondSourceTask();
    SecondComputeTask computeTask = new SecondComputeTask();
    SecondSinkTask sinkTask = new SecondSinkTask("secondgraphpoints");

    ComputeGraphBuilder secondGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
    secondGraphBuilder.addSource("secondsource", sourceTask, parallelism);
    ComputeConnection computeConnection = secondGraphBuilder.addCompute(
        "secondcompute", computeTask, parallelism);
    ComputeConnection sinkConnection = secondGraphBuilder.addSink(
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
    ThirdSinkTask sinkTask = new ThirdSinkTask();

    ComputeGraphBuilder thirdGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
    thirdGraphBuilder.addSource("thirdsource", sourceTask, parallelism);
    ComputeConnection sinkConnection = thirdGraphBuilder.addSink(
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
  public void execute(Config config, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {

    long startTime = System.currentTimeMillis();

    LOG.log(Level.FINE, "Task worker starting: " + workerID);

    ComputeEnvironment cEnv = ComputeEnvironment.init(config, workerID, workerController,
        persistentVolume, volatileVolume);
    TaskExecutor taskExecutor = cEnv.getTaskExecutor();

    ComputeGraph firstGraph = buildFirstGraph(2, config);
    ComputeGraph secondGraph = buildSecondGraph(2, config);
    ComputeGraph thirdGraph = buildThirdGraph(2, config);

    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(firstGraph);
    taskExecutor.execute(firstGraph, firstGraphExecutionPlan);
    DataObject<Object> firstGraphObject = taskExecutor.getOutput(
        firstGraph, firstGraphExecutionPlan, "firstsink");

    //Get the execution plan for the second task graph
    ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(secondGraph);
    taskExecutor.execute(secondGraph, secondGraphExecutionPlan);
    DataObject<Object> secondGraphObject = taskExecutor.getOutput(
        secondGraph, secondGraphExecutionPlan, "secondsink");

    ExecutionPlan plan = taskExecutor.plan(thirdGraph);
    taskExecutor.addInput(
        thirdGraph, plan, "thirdsource", "firstgraphpoints", firstGraphObject);
    taskExecutor.addInput(
        thirdGraph, plan, "thirdsource", "secondgraphpoints", secondGraphObject);
    taskExecutor.execute(thirdGraph, plan);

    DataObject<Object> thirdGraphObject = taskExecutor.getOutput(thirdGraph, plan, "thirdsink");
    LOG.info("%%%%%%%%% Third Graph Object:" + thirdGraphObject);
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

      double[] newPoints = new double[datapoints.length];
      if (context.taskIndex() == 0) {
        for (int i = 0; i < datapoints.length / 2; i++) {
          newPoints[i] = datapoints[i];
        }
        context.writeEnd(Context.TWISTER2_DIRECT_EDGE, newPoints);
      } else if (context.taskIndex() == 1) {
        for (int i = datapoints.length / 2; i < datapoints.length / 2; i++) {
          newPoints[i] = datapoints[i];
        }
        context.writeEnd(Context.TWISTER2_DIRECT_EDGE, newPoints);
      }
    }

    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
      numPoints = Integer.parseInt(cfg.getStringValue(DataObjectConstants.DSIZE));
      LOG.info("Number of Points:" + numPoints);
    }
  }

  private static class FirstComputeTask extends BaseCompute {
    private static final long serialVersionUID = -254264120110286748L;

    FirstComputeTask() {
    }

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received Points: " + context.getWorkerId()
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

  private static class FirstSinkTask extends BaseSink implements Collector {
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
      return new EntityPartition<>(context.taskIndex(), datapoints);
    }

    @Override
    public Set<String> getCollectibleNames() {
      return Collections.singleton(inputKey);
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
      LOG.log(Level.INFO, "Received Points: " + context.getWorkerId()
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

  private static class SecondSinkTask extends BaseSink implements Collector {
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
      return new EntityPartition<>(context.taskIndex(), datapoints);
    }

    @Override
    public Set<String> getCollectibleNames() {
      return Collections.singleton(inputKey);
    }
  }

  private static class ThirdSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private double[] firstDatapoints = null;
    private double[] secondDatapoints = null;
    private double[] calculatedValues = null;

    private DataObject<?> firstPointsObject = null;
    private DataObject<?> secondPointsObject = null;

    ThirdSourceTask() {
    }

    @Override
    public void execute() {

      DataPartition<?> firstDataPartition = firstPointsObject.getPartition(context.taskIndex());
      firstDatapoints = (double[]) firstDataPartition.getConsumer().next();

      DataPartition<?> secondDataPartition = secondPointsObject.getPartition(context.taskIndex());
      secondDatapoints = (double[]) secondDataPartition.getConsumer().next();

      calculatedValues = new double[firstDatapoints.length];

      LOG.info("Context Task Index:" + context.taskIndex()
          + "\tFirst data points length:" + firstDatapoints.length
          + "\t\nSecond data points length:" + secondDatapoints.length);

      for (int i = 0; i < firstDatapoints.length; i++) {
        for (int j = 0; j < secondDatapoints.length; j++) {
          calculatedValues[i] = firstDatapoints[i] + secondDatapoints[j];
        }
      }

      LOG.info("Context Task Index:" + context.taskIndex()
          + "\tCalculated Values Length:" + calculatedValues.length
          + "\t\nCalculated Values:" + Arrays.toString(calculatedValues));
      context.writeEnd("all-reduce", calculatedValues);
    }

    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
    }

    @Override
    public void add(String name, DataObject<?> data) {
      if ("firstgraphpoints".equals(name)) {
        this.firstPointsObject = data;
      }
      if ("secondgraphpoints".equals(name)) {
        this.secondPointsObject = data;
      }
    }

    @Override
    public Set<String> getReceivableNames() {
      Set<String> inputKeys = new HashSet<>();
      inputKeys.add("firstgraphpoints");
      inputKeys.add("secondgraphpoints");
      return inputKeys;
    }
  }

  private static class ThirdSinkTask extends BaseSink implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    private double[] datapoints;
    private String inputKey;

    ThirdSinkTask() {
    }

    @Override
    public boolean execute(IMessage message) {
      datapoints = (double[]) message.getContent();
      return true;
    }

    @Override
    public DataPartition<double[]> get() {
      return new EntityPartition<>(context.taskIndex(), datapoints);
    }

    @Override
    public DataPartition<double[]> get(String name) {
      return null;
    }

    @Override
    public Set<String> getCollectibleNames() {
      return Collections.singleton(inputKey);
    }
  }

  private static class Aggregator implements IFunction {
    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public Object onMessage(Object object1, Object object2) throws ArrayIndexOutOfBoundsException {

      double[] object11 = (double[]) object1;
      double[] object21 = (double[]) object2;

      LOG.info("object 1 and object 2:"
          + Arrays.toString(object11) + "\t\n" + Arrays.toString(object21));

      double[] object31 = new double[object11.length];

      for (int j = 0; j < object11.length; j++) {
        double newVal = object11[j] + object21[j];
        object31[j] = newVal;
      }

      LOG.info("Object 3 len:" + object31.length
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
