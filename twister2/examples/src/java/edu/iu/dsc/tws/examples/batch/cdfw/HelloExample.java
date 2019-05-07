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
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.cdfw.BaseDriver;
import edu.iu.dsc.tws.api.cdfw.CDFWEnv;
import edu.iu.dsc.tws.api.cdfw.DafaFlowJobConfig;
import edu.iu.dsc.tws.api.cdfw.DataFlowGraph;
import edu.iu.dsc.tws.api.cdfw.task.ConnectedSink;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public final class HelloExample {
  private static final Logger LOG = Logger.getLogger(HelloExample.class.getName());

  private HelloExample() {
  }

  public static class HelloDriver extends BaseDriver {

    @Override
    public void execute(CDFWEnv execEnv) {
      // build JobConfig
      DafaFlowJobConfig dafaFlowJobConfig = new DafaFlowJobConfig();
      FirstSource firstSource = new FirstSource();
      SecondSink secondSink = new SecondSink();
      TaskGraphBuilder graphBuilderX = TaskGraphBuilder.newBuilder(execEnv.getConfig());
      graphBuilderX.addSource("source1", firstSource, 4);
      ComputeConnection reduceConn = graphBuilderX.addSink("sink1", secondSink,
          1);
      reduceConn.reduce("source1", "all-reduce", new Aggregator(),
          DataType.OBJECT);

      graphBuilderX.setMode(OperationMode.BATCH);
      DataFlowTaskGraph batchGraph = graphBuilderX.build();

      //Invoke CDFW Submitter and send the metagraph
      DataFlowGraph job = DataFlowGraph.newSubGraphJob("hello", batchGraph).
          setWorkers(4).addDataFlowJobConfig(dafaFlowJobConfig);
      execEnv.executeDataFlowGraph(job);
    }
  }

  private static class FirstSource extends BaseSource {
    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public void execute() {
      context.writeEnd("all-reduce", "Hello");
    }
  }

  private static class SecondSink extends ConnectedSink implements Collector {
    private static final long serialVersionUID = -5190777711234234L;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received centroids: " + context.getWorkerId()
          + ":" + context.globalTaskId() + message.getContent());
      return true;
    }

    @Override
    public DataPartition<Object> get() {
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
        .put(SchedulerContext.DRIVER_CLASS, null).build();

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newCDFWBuilder()
        .setJobName(HelloExample.class.getName())
        .setDriverClass(HelloDriver.class.getName())
        .addComputeResource(1, 512, instances)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}

