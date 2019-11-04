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
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.dataset.partition.CollectionPartition;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.task.cdfw.BaseDriver;
import edu.iu.dsc.tws.task.cdfw.CDFWEnv;
import edu.iu.dsc.tws.task.cdfw.DataFlowGraph;
import edu.iu.dsc.tws.task.cdfw.DataFlowJobConfig;
import edu.iu.dsc.tws.task.cdfw.task.ConnectedSink;
import edu.iu.dsc.tws.task.cdfw.task.ConnectedSource;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.cdfw.CDFWWorker;

public final class TwoDataFlowsExample {
  private static final Logger LOG = Logger.getLogger(TwoDataFlowsExample.class.getName());

  private TwoDataFlowsExample() {
  }

  public static class TwoDataFlowsDriver extends BaseDriver {
    @Override
    public void execute(CDFWEnv exec) {
      // build JobConfig
      DataFlowJobConfig jobConfig = new DataFlowJobConfig();
      LOG.log(Level.INFO, "Executing the first graph");
      // run the first job
      runFirstJob(exec.getConfig(), exec, 4, jobConfig);
      // run the second job
      LOG.log(Level.INFO, "Executing the second graph");
      runSecondJob(exec.getConfig(), exec, 4, jobConfig);
    }
  }

  private static class FirstSourceTask extends BaseSource {
    private static final long serialVersionUID = -254264120110286748L;

    private CollectionPartition<Object> collectionPartition;

    protected FirstSourceTask() {
    }

    @Override
    public void execute() {
      LOG.fine("Context task id and index:" + context.taskId() + "\t" + context.taskIndex());
      for (int i = 0; i < 4; i++) {
        collectionPartition.add("PartitionValue" + i);
      }
      context.writeEnd("partition", collectionPartition);
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
      collectionPartition = new CollectionPartition();
    }
  }

  /**
   * This class aggregates the cluster centroid values and sum the new centroid values.
   */
  public static class Aggregator implements IFunction {
    private static final long serialVersionUID = -254264120110286748L;

    /**
     * The actual message callback
     *
     * @param object1 the actual message
     * @param object2 the actual message
     */
    @Override
    public Object onMessage(Object object1, Object object2) throws ArrayIndexOutOfBoundsException {
      return object1.toString() + object2.toString();
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

    @SuppressWarnings("deprecation")
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine = commandLineParser.parse(options, args);

    int instances = Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_WORKERS));
    int parallelismValue =
        Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_PARALLELISM_VALUE));

    configurations.put(CDFConstants.ARGS_WORKERS, Integer.toString(instances));
    configurations.put(CDFConstants.ARGS_PARALLELISM_VALUE, Integer.toString(parallelismValue));

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    config = Config.newBuilder().putAll(config)
        .put(SchedulerContext.DRIVER_CLASS, null).build();

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(HelloExample.class.getName())
        .setWorkerClass(CDFWWorker.class)
        .setDriverClass(TwoDataFlowsDriver.class.getName())
        .addComputeResource(1, 512, instances)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }

  private static void runFirstJob(Config config, CDFWEnv cdfwEnv,
                                  int parallelism, DataFlowJobConfig jobConfig) {
    FirstSourceTask firstSourceTask = new FirstSourceTask();
    ConnectedSink connectedSink = new ConnectedSink("first_out");

    ComputeGraphBuilder graphBuilderX = ComputeGraphBuilder.newBuilder(config);
    graphBuilderX.addSource("source1", firstSourceTask, parallelism);
    ComputeConnection partitionConnection = graphBuilderX.addSink("sink1", connectedSink,
        parallelism);
    partitionConnection.partition("source1")
        .viaEdge("partition")
        .withDataType(MessageTypes.OBJECT);

    graphBuilderX.setMode(OperationMode.BATCH);
    ComputeGraph batchGraph = graphBuilderX.build();

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("first_graph", batchGraph)
        .setWorkers(4).addDataFlowJobConfig(jobConfig)
        .setGraphType("non-iterative");
    cdfwEnv.executeDataFlowGraph(job);
  }

  private static void runSecondJob(Config config, CDFWEnv cdfwEnv, int parallelism,
                                   DataFlowJobConfig jobConfig) {

    ConnectedSource connectedSource = new ConnectedSource("reduce", "first_out");
    ConnectedSink connectedSink = new ConnectedSink();

    ComputeGraphBuilder graphBuilderX = ComputeGraphBuilder.newBuilder(config);
    graphBuilderX.addSource("source2", connectedSource, parallelism);
    ComputeConnection reduceConn = graphBuilderX.addSink("sink2", connectedSink, 1);
    reduceConn.reduce("source2")
        .viaEdge("reduce")
        .withReductionFunction(new Aggregator())
        .withDataType(MessageTypes.OBJECT);

    graphBuilderX.setMode(OperationMode.BATCH);
    ComputeGraph batchGraph = graphBuilderX.build();

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("second_graph", batchGraph)
        .setWorkers(4).addDataFlowJobConfig(jobConfig)
        .setGraphType("non-iterative");
    cdfwEnv.executeDataFlowGraph(job);
  }
}
