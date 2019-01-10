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
import edu.iu.dsc.tws.api.cdfw.CDFWExecutor;
import edu.iu.dsc.tws.api.cdfw.DataFlowGraph;
import edu.iu.dsc.tws.api.cdfw.Twister2HTGDriver;
import edu.iu.dsc.tws.api.cdfw.task.ConnectedSink;
import edu.iu.dsc.tws.api.cdfw.task.ConnectedSource;
import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DSet;
import edu.iu.dsc.tws.dataset.Partition;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public final class TwoDataFlowsExample {
  private static final Logger LOG = Logger.getLogger(TwoDataFlowsExample.class.getName());

  private TwoDataFlowsExample() {
  }

  private static class HTGSourceTask extends BaseSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public void execute() {
      context.writeEnd("all-reduce", "Hello");
    }

    @Override
    public void add(String name, DSet<Object> data) {
      LOG.log(Level.FINE, "Received input: " + name);
    }
  }

  private static class HTGReduceTask extends BaseSink implements Collector<Object> {
    private static final long serialVersionUID = -5190777711234234L;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received centroids: " + context.getWorkerId()
          + ":" + context.taskId() + message.getContent());
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
        .put(SchedulerContext.DRIVER_CLASS, Twister2HTGDriver.class.getName()).build();

    CDFWExecutor cdfwExecutor = new CDFWExecutor(config);
    LOG.log(Level.INFO, "Executing the first graph");
    // run the first job
    runFirstJob(config, cdfwExecutor, parallelismValue, jobConfig);
    // run the second job
    LOG.log(Level.INFO, "Executing the second graph");
    runSecondJob(config, cdfwExecutor, parallelismValue, jobConfig);
    cdfwExecutor.close();
  }

  private static CDFWExecutor runFirstJob(Config config, CDFWExecutor cdfwExecutor,
                                          int parallelismValue, JobConfig jobConfig) {

    HTGSourceTask htgSourceTask = new HTGSourceTask();
    ConnectedSink htgReduceTask = new ConnectedSink();

    TaskGraphBuilder graphBuilderX = TaskGraphBuilder.newBuilder(config);
    graphBuilderX.addSource("source1", htgSourceTask, parallelismValue);
    ComputeConnection partitionConnection = graphBuilderX.addSink("sink1", htgReduceTask,
        parallelismValue);
    partitionConnection.partition("source1", "partition",
        DataType.OBJECT);

    graphBuilderX.setMode(OperationMode.BATCH);
    DataFlowTaskGraph batchGraph = graphBuilderX.build();

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("second", batchGraph).
        setWorkers(4).addJobConfig(jobConfig);
    cdfwExecutor.execute(job);
    return cdfwExecutor;
  }

  private static CDFWExecutor runSecondJob(Config config, CDFWExecutor cdfwExecutor,
                                          int parallelismValue, JobConfig jobConfig) {

    ConnectedSource htgSourceTask = new ConnectedSource();
    ConnectedSink htgReduceTask = new ConnectedSink();

    TaskGraphBuilder graphBuilderX = TaskGraphBuilder.newBuilder(config);
    graphBuilderX.addSource("source1", htgSourceTask, parallelismValue);
    ComputeConnection reduceConn = graphBuilderX.addSink("sink1", htgReduceTask,
        1);
    reduceConn.reduce("source1", "reduce", new Aggregator(),
        DataType.OBJECT);

    graphBuilderX.setMode(OperationMode.BATCH);
    DataFlowTaskGraph batchGraph = graphBuilderX.build();

    DataFlowGraph job = DataFlowGraph.newSubGraphJob("first", batchGraph).
        setWorkers(4).addJobConfig(jobConfig);
    cdfwExecutor.execute(job);
    return cdfwExecutor;
  }
}
