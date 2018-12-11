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
package edu.iu.dsc.tws.examples.batch.htg;

import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.htgjob.Twister2HTGSubmitter;
import edu.iu.dsc.tws.api.htgjob.Twister2MetagraphBuilder;
import edu.iu.dsc.tws.api.htgjob.Twister2MetagraphConnection;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.api.task.htg.HTGBuilder;
import edu.iu.dsc.tws.api.task.htg.HTGComputeConnection;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataSet;
import edu.iu.dsc.tws.dataset.Partition;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.batch.BaseBatchSink;
import edu.iu.dsc.tws.task.batch.BaseBatchSource;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.task.graph.htg.HierarchicalTaskGraph;
import edu.iu.dsc.tws.tsched.utils.HTGParser;

public class HTGExample extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(HTGExample.class.getName());

  private HTGJobParameters jobParameters;

  private static final long serialVersionUID = -5190777711234234L;

  @Override
  public void execute() {

    HTGSourceTask htgSourceTask = new HTGSourceTask();
    HTGReduceTask htgReduceTask = new HTGReduceTask();

    this.jobParameters = HTGJobParameters.build(config);
    int parallelismValue = jobParameters.getParallelismValue();

    TaskGraphBuilder graphBuilderX = TaskGraphBuilder.newBuilder(config);
    graphBuilderX.addSource("source1", htgSourceTask, parallelismValue);
    ComputeConnection computeConnection1 = graphBuilderX.addSink("sink1", htgReduceTask,
        parallelismValue);
    computeConnection1.allreduce("source1", "all-reduce", new Aggregator(),
        DataType.OBJECT);
    graphBuilderX.setMode(OperationMode.STREAMING);
    DataFlowTaskGraph batchGraph = graphBuilderX.build();

    TaskGraphBuilder graphBuilderY = TaskGraphBuilder.newBuilder(config);
    graphBuilderY.addSource("source2", htgSourceTask, parallelismValue);
    ComputeConnection computeConnection2 = graphBuilderY.addSink("sink2", htgReduceTask,
        parallelismValue);
    computeConnection2.allreduce("source2", "all-reduce", new Aggregator(),
        DataType.OBJECT);
    graphBuilderY.setMode(OperationMode.BATCH);
    DataFlowTaskGraph streamingGraph = graphBuilderY.build();

    HTGBuilder htgBuilder = HTGBuilder.newBuilder(config);
    htgBuilder.addSourceTaskGraph("sourcetaskgraph", batchGraph);
    HTGComputeConnection htgComputeConnection = htgBuilder.addSinkTaskGraph(
        "sinktaskgraph", streamingGraph, "source2");
    htgComputeConnection.partition("sourcetaskgraph", "sink1");
    htgBuilder.setMode(OperationMode.BATCH);

    HierarchicalTaskGraph hierarchicalTaskGraph = htgBuilder.buildHierarchicalTaskGraph();

    LOG.info("Batch Task Graph:" + batchGraph.getTaskVertexSet() + "\t"
        + batchGraph.getTaskVertexSet().size() + "\t"
        + "Streaming Task Graph:" + streamingGraph.getTaskVertexSet() + "\t"
        + streamingGraph.getTaskVertexSet().size());

    //Invoke HTG Parser
    HTGParser hierarchicalTaskGraphParser = new HTGParser(hierarchicalTaskGraph);
    List<DataFlowTaskGraph> dataFlowTaskGraphList =
        hierarchicalTaskGraphParser.hierarchicalTaskGraphParse();

    dataFlowTaskGraphList.stream().map(
        graph -> "dataflow task graph list:" + graph.getTaskGraphName()).forEach(LOG::info);

    //Select it from the list...!
    ExecutionPlan plan = taskExecutor.plan(batchGraph);
    taskExecutor.execute(batchGraph, plan);
  }//End of execute method

  public static void sleep(long duration) {
    LOG.info("Sleeping " + duration + "ms............");
    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static class HTGSourceTask extends BaseBatchSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public void execute() {
      context.writeEnd("all-reduce", "Hello");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void add(String name, DataSet<Object> data) {
      LOG.log(Level.FINE, "Received input: " + name);
    }
  }

  private static class HTGReduceTask extends BaseBatchSink implements Collector<Object> {
    private static final long serialVersionUID = -5190777711234234L;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.FINE, "Received centroids: " + context.getWorkerId()
          + ":" + context.taskId());
      return true;
    }

    @Override
    public Partition<Object> get() {
      return null;
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
      return null;
    }
  }

  public static void main(String[] args) throws ParseException {

    LOG.log(Level.INFO, "HTG Graph Job");

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    Options options = new Options();
    options.addOption(HTGConstants.ARGS_PARALLELISM_VALUE, true, "2");
    options.addOption(HTGConstants.ARGS_WORKERS, true, "2");

    @SuppressWarnings("deprecation")
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine = commandLineParser.parse(options, args);

    int instances = Integer.parseInt(commandLine.getOptionValue(HTGConstants.ARGS_WORKERS));
    int parallelismValue =
        Integer.parseInt(commandLine.getOptionValue(HTGConstants.ARGS_PARALLELISM_VALUE));

    configurations.put(HTGConstants.ARGS_WORKERS, Integer.toString(instances));
    configurations.put(HTGConstants.ARGS_PARALLELISM_VALUE, Integer.toString(parallelismValue));

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("HTG");
    jobBuilder.setWorkerClass(HTGExample.class.getName());
    jobBuilder.setConfig(jobConfig);
    jobBuilder.addComputeResource(2.0, 512, 2);

    // now submit the job
    //Twister2Submitter.submitJob(jobBuilder.build(), config);

    //TODO:Design the metagraph
    Twister2MetagraphBuilder twister2MetagraphBuilder = Twister2MetagraphBuilder.newBuilder(config);
    twister2MetagraphBuilder.addSource("sourcetaskgraph", config);
    Twister2MetagraphConnection twister2MetagraphConnection = twister2MetagraphBuilder.addSink(
        "sinktaskgraph", config);
    twister2MetagraphConnection.broadcast("sourcetaskgraph", "broadcast");
    twister2MetagraphBuilder.setHtgName("htg");

    //TODO:Invoke HTG Client and send the metagraph -> start with FIFO
    Twister2HTGSubmitter twister2HTGSubmitter = new Twister2HTGSubmitter(config);
    twister2HTGSubmitter.execute(twister2MetagraphBuilder.build(),
        jobConfig, HTGExample.class.getName());

    /*Twister2MetaGraph twister2MetaGraph = twister2MetagraphBuilder.build();
    Twister2MetaGraph.SubGraph subGraph = Twister2HTGClient.execute(twister2MetaGraph);
    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName(subGraph.getName());
    jobBuilder.setWorkerClass(HTGExample.class.getName());
    jobBuilder.setConfig(jobConfig);
    jobBuilder.addComputeResource(subGraph.getCpu(), subGraph.getRamMegaBytes(),
        subGraph.getDiskGigaBytes(), subGraph.getNumberOfInstances());

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);*/
  }
}

