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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;

public class BatchTaskSchedulerExample extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(BatchTaskSchedulerExample.class.getName());

  private static int parallelismValue;
  private static int workers;
  private static int dsize;

  public static ComputeGraph buildFirstGraph(int parallelism,
                                             Config conf) {

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    SourceTask sourceTask = new SourceTask();
    ComputeTask computeTask = new ComputeTask();
    ReduceTask reduceTask = new ReduceTask("firstgraphpoints");

    ComputeGraphBuilder firstGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
    firstGraphBuilder.addSource("firstsource", sourceTask, parallelism);
    ComputeConnection computeConnection = firstGraphBuilder.addCompute(
        "firstcompute", computeTask, parallelism);
    ComputeConnection sinkConnection = firstGraphBuilder.addSink(
        "firstsink", reduceTask, parallelism);

    //Creating the communication edges between the tasks for the second task graph
    computeConnection.direct("firstsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    sinkConnection.allreduce("firstcompute")
        .viaEdge("all-reduce")
        .withReductionFunction(new Aggregator())
        .withDataType(MessageTypes.OBJECT);
    firstGraphBuilder.setMode(OperationMode.BATCH);
    firstGraphBuilder.setTaskGraphName("firstTG");
    //Build the first taskgraph
    return firstGraphBuilder.build();
  }

  public static ComputeGraph buildSecondGraph(int parallelism,
                                              Config conf) {
    //Add source, compute, and sink tasks to the task graph builder for the second task graph
    SourceTask sourceTask = new SourceTask();
    ComputeTask computeTask = new ComputeTask();
    ReduceTask reduceTask = new ReduceTask("secondgraphpoints");

    ComputeGraphBuilder firstGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
    firstGraphBuilder.addSource("secondsource", sourceTask, parallelism);
    ComputeConnection computeConnection = firstGraphBuilder.addCompute(
        "secondcompute", computeTask, parallelism);
    ComputeConnection sinkConnection = firstGraphBuilder.addSink(
        "secondsink", reduceTask, parallelism);

    //Creating the communication edges between the tasks for the second task graph
    computeConnection.direct("secondsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    sinkConnection.allreduce("secondcompute")
        .viaEdge("all-reduce")
        .withReductionFunction(new Aggregator())
        .withDataType(MessageTypes.OBJECT);

    firstGraphBuilder.setMode(OperationMode.BATCH);
    firstGraphBuilder.setTaskGraphName("secondTG");
    //Build the first taskgraph
    return firstGraphBuilder.build();
  }

  public static ComputeGraph buildThirdGraph(int parallelism, Config conf) {

    //Add source, compute, and sink tasks to the task graph builder for the third task graph
    SourceTask sourceTask = new SourceTask();
    ComputeTask computeTask = new ComputeTask();
    ReduceTask reduceTask = new ReduceTask("thirdgraphpoints");

    ComputeGraphBuilder thirdGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
    thirdGraphBuilder.addSource("thirdsource", sourceTask, parallelism);
    ComputeConnection computeConnection = thirdGraphBuilder.addCompute(
        "thirdcompute", computeTask, parallelism);
    ComputeConnection sinkConnection = thirdGraphBuilder.addSink(
        "thirdsink", reduceTask, parallelism);

    //Creating the communication edges between the tasks for the third task graph
    computeConnection.direct("thirdsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    sinkConnection.allreduce("thirdcompute")
        .viaEdge("all-reduce")
        .withReductionFunction(new Aggregator())
        .withDataType(MessageTypes.OBJECT);

    thirdGraphBuilder.setMode(OperationMode.BATCH);
    thirdGraphBuilder.setTaskGraphName("thirdTG");
    //Build the first taskgraph
    return thirdGraphBuilder.build();
  }

  public static class SourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private double[] datapoints = null;
    private int numPoints = 0;

    public SourceTask() {
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
      if ("firstgraphpoints".equals(name)) {
        LOG.info("points value:" + name);
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

  public static class ComputeTask extends BaseCompute  {
    private static final long serialVersionUID = -254264120110286748L;

    private int count = 0;

    public ComputeTask() {
    }

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.FINE, "Received Points: " + context.getWorkerId()
          + ":" + context.globalTaskId());
      if (message.getContent() instanceof Iterator) {
        Iterator it = (Iterator) message.getContent();
        while (it.hasNext()) {
          count += 1;
          context.write("all-reduce", it.next());
        }
      }
      context.end("all-reduce");
      return true;
    }
  }

  public static class ReduceTask extends BaseSink implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    private double[] datapoints;
    private String inputKey;

    public ReduceTask() {
    }

    public ReduceTask(String inputkey) {
      this.inputKey = inputkey;
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

  public static class Aggregator implements IFunction {
    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public Object onMessage(Object object1, Object object2) throws ArrayIndexOutOfBoundsException {

      double[] object11 = (double[]) object1;
      double[] object21 = (double[]) object2;

      double[] object31 = new double[object11.length];

      for (int j = 0; j < object11.length; j++) {
        double newVal = object11[j] + object21[j];
        object31[j] = newVal;
      }
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

    workers = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.WORKERS));
    parallelismValue = Integer.parseInt(cmd.getOptionValue(
        DataObjectConstants.PARALLELISM_VALUE));
    dsize = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.DSIZE));

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

  @Override
  public void execute() {

    long startTime = System.currentTimeMillis();

    ComputeGraph firstGraph = buildFirstGraph(parallelismValue, config);

    //ComputeGraph secondGraph = buildSecondGraph(parallelismValue, config);

    //ComputeGraph thirdGraph = buildThirdGraph(parallelismValue, config);

    //Get the execution plan for the dependent task graphs
//    Map<String, ExecutionPlan> taskSchedulePlanMap = cEnv.build(
//        firstGraph, secondGraph, thirdGraph);

    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(firstGraph);

    //Using the map we can get the execution plan for the individual task graphs
    //ExecutionPlan firstGraphExecutionPlan = taskSchedulePlanMap.get(firstGraph.getGraphName());

    //Actual execution for the first taskgraph
    taskExecutor.execute(firstGraph, firstGraphExecutionPlan);

    //Retrieve the output of the first task graph
    /*DataObject<Object> firstGraphObject = taskExecutor.getOutput(
        firstGraph, firstGraphExecutionPlan, "firstsink");*/

    //Get the execution plan for the second task graph
    //ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(secondGraph);

    //taskExecutor.execute(secondGraph, secondGraphExecutionPlan);

    //Retrieve the output of the first task graph
    /*DataObject<Object> secondGraphObject = taskExecutor.getOutput(
        secondGraph, secondGraphExecutionPlan, "secondsink");*/

    long endTimeData = System.currentTimeMillis();

    //Perform the iterations from 0 to 'n' number of iterations
    //ExecutionPlan plan = taskSchedulePlanMap.get(kmeansTaskGraph.getGraphName());
    //ExecutionPlan plan = taskExecutor.plan(thirdGraph);
    /*taskExecutor.addInput(
        thirdGraph, plan, "thirdsource", "firstgraphpoints", firstGraphObject);
    taskExecutor.addInput(
        thirdGraph, plan, "thirdsource", "secondgraphpoints", secondGraphObject);*/
    //actual execution of the third task graph
    //taskExecutor.execute(thirdGraph, plan);

    //TODO:Retrieve the output of the graph

    long endTime = System.currentTimeMillis();

    LOG.info("Total Execution Time: " + (endTime - startTime)
        + "\tData Load time : " + (endTimeData - startTime)
        + "\tCompute Time : " + (endTime - endTimeData));
  }
}
