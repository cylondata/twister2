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
package edu.iu.dsc.tws.examples.internal.taskgraph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.BaseCompute;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

/**
 * This is the example for testing the task graph which consists of a source sending data to
 * multiple compute elements. Also, the sink/reduce task receive the input from multiple compute
 * elements. The structure of the graph is as given below:
 * <p>
 * SourceTask (Two Outgoing Edges)
 * |           | (Direct Communication)
 * V           V
 * FirstComputeTask  SecondComputeTask
 * |            | (All-Reduce Communication)
 * V            V
 * ReduceTask (Two Incoming Edges)
 */

public class MultiComputeTaskGraphExample extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(MultiComputeTaskGraphExample.class.getName());

  @Override
  public void execute() {

    LOG.log(Level.INFO, "Task worker starting: " + workerId);
    int parallel = 2;

    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(config);

    SourceTask sourceTask = new SourceTask();
    FirstComputeTask firstComputeTask = new FirstComputeTask();
    SecondComputeTask secondComputeTask = new SecondComputeTask();
    ReduceTask reduceTask = new ReduceTask();

    builder.addSource("source", sourceTask, parallel);
    ComputeConnection firstComputeConnection = builder.addCompute(
        "firstcompute", firstComputeTask, parallel);
    ComputeConnection secondComputeConnection = builder.addCompute(
        "secondcompute", secondComputeTask, parallel);
    ComputeConnection reduceConnection = builder.addSink("sink", reduceTask, parallel);

    firstComputeConnection.direct("source", "fdirect", DataType.OBJECT);
    secondComputeConnection.direct("source", "sdirect", DataType.OBJECT);
    reduceConnection.allreduce("firstcompute", "freduce", new Aggregator(), DataType.OBJECT);
    reduceConnection.allreduce("secondcompute", "sreduce", new Aggregator(), DataType.OBJECT);

    builder.setMode(OperationMode.BATCH);
    DataFlowTaskGraph graph = builder.build();
    ExecutionPlan plan = taskExecutor.plan(graph);
    taskExecutor.execute(graph, plan);

    DataObject<double[]> dataSet = taskExecutor.getOutput(graph, plan, "sink");
    DataPartition<double[]> values = dataSet.getPartitions()[0];
    double[] newValue = values.getConsumer().next();
    LOG.info("Final Aggregated Values Are:" + Arrays.toString(newValue));
  }

  /**
   * This class is just generating 'n' number of datapoints and write into the first and second
   * compute task using direct communication.
   */
  private static class SourceTask extends BaseSource {
    private static final long serialVersionUID = -254264120110286748L;

    private double[] datapoints = null;
    private int numPoints = 100;

    @Override
    public void execute() {
      int seedValue = 100;
      datapoints = new double[numPoints];
      Random r = new Random(seedValue);
      for (int i = 0; i < numPoints; i++) {
        double randomValue = r.nextDouble();
        datapoints[i] = randomValue;
      }
      context.write("fdirect", datapoints);
      context.write("sdirect", datapoints);
      context.end("fdirect");
      context.end("sdirect");
    }

    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
    }
  }

  /**
   * This class receives the datapoints and just do simple calculation of multiply the data points
   * by '2' and send to the reduce task using all-reduce communication.
   */
  private static class FirstComputeTask extends BaseCompute {

    private static final long serialVersionUID = -254264120110286748L;

    private int dsize = 100;
    private double[] cal = new double[dsize];

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "First Compute Received Data: " + context.getWorkerId()
          + ":" + context.taskId());
      if (message.getContent() instanceof Iterator) {
        while (((Iterator) message.getContent()).hasNext()) {
          Object ret = ((Iterator) message.getContent()).next();
          cal = (double[]) ret;
          for (int i = 0; i < cal.length; i++) {
            cal[i] = cal[i] * 2.0;
          }
          context.write("freduce", cal);
        }
      }
      context.end("freduce");
      return true;
    }
  }

  /**
   * This class receives the datapoints and just do simple calculation of divide the data points
   * by '4' and send to the reduce task using all-reduce communication.
   */
  private static class SecondComputeTask extends BaseCompute {
    private static final long serialVersionUID = -254264120110286748L;

    private int dsize = 100;
    private double[] cal = new double[dsize];

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Second Compute Received Data: " + context.getWorkerId()
          + ":" + context.taskId());
      if (message.getContent() instanceof Iterator) {
        while (((Iterator) message.getContent()).hasNext()) {
          Object ret = ((Iterator) message.getContent()).next();
          cal = (double[]) ret;
          for (int i = 0; i < cal.length; i++) {
            cal[i] = cal[i] / 4.0;
          }
          context.write("sreduce", cal);
        }
      }
      context.end("sreduce");
      return true;
    }
  }

  private static class ReduceTask extends BaseSink implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    private double[] newValues;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received Data from workerId: " + context.getWorkerId()
          + ":" + context.taskId() + ":" + message.getContent());
      newValues = (double[]) message.getContent();
      return true;
    }

    @Override
    public DataPartition<double[]> get() {
      return new EntityPartition<>(context.taskIndex(), newValues);
    }
  }

  public class Aggregator implements IFunction {
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
    LOG.log(Level.INFO, "MultiComputeTaskGraph");

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    Options options = new Options();
    options.addOption(DataObjectConstants.ARGS_PARALLELISM_VALUE, true, "parallelism");
    options.addOption(DataObjectConstants.ARGS_WORKERS, true, "workers");
    options.addOption(DataObjectConstants.ARGS_DSIZE, true, "dsize");

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    int workers = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.ARGS_WORKERS));
    int parallelismValue = Integer.parseInt(cmd.getOptionValue(
        DataObjectConstants.ARGS_PARALLELISM_VALUE));
    int dsize = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.ARGS_DSIZE));

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put(DataObjectConstants.ARGS_DSIZE, Integer.toString(dsize));
    jobConfig.put(DataObjectConstants.ARGS_WORKERS, Integer.toString(workers));
    jobConfig.put(DataObjectConstants.ARGS_PARALLELISM_VALUE, Integer.toString(parallelismValue));
    jobConfig.putAll(configurations);

    //build the job
    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("MultiComputeTaskGraph");
    jobBuilder.setWorkerClass(MultiComputeTaskGraphExample.class.getName());
    jobBuilder.addComputeResource(2, 512, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
