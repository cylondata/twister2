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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.htgjob.SubGraphJob;
import edu.iu.dsc.tws.api.htgjob.Twister2HTGDriver;
import edu.iu.dsc.tws.api.htgjob.Twister2HTGSubmitter;
import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataSet;
import edu.iu.dsc.tws.dataset.Partition;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public final class HTGExample {
  private static final Logger LOG = Logger.getLogger(HTGExample.class.getName());

  private HTGExample() {
  }

  private static class HTGSourceTask extends BaseSource implements Receptor {
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

  private static class HTGReduceTask extends BaseSink implements Collector<Object> {
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

    LOG.log(Level.INFO, "HTG Job");

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

    config = Config.newBuilder().putAll(config)
        .put(SchedulerContext.DRIVER_CLASS, Twister2HTGDriver.class.getName()).build();


    HTGSourceTask htgSourceTask = new HTGSourceTask();
    HTGReduceTask htgReduceTask = new HTGReduceTask();
    TaskGraphBuilder graphBuilderX = TaskGraphBuilder.newBuilder(config);
    graphBuilderX.addSource("source1", htgSourceTask, parallelismValue);
    ComputeConnection reduceConn = graphBuilderX.addSink("sink1", htgReduceTask,
        parallelismValue);
    reduceConn.allreduce("source1", "all-reduce", new Aggregator(),
        DataType.OBJECT);

    graphBuilderX.setMode(OperationMode.BATCH);
    DataFlowTaskGraph batchGraph = graphBuilderX.build();

    //Invoke HTG Submitter and send the metagraph
    Twister2HTGSubmitter twister2HTGSubmitter = new Twister2HTGSubmitter(config);
    SubGraphJob job = SubGraphJob.newSubGraphJob(batchGraph).setWorkers(4);
    twister2HTGSubmitter.execute(job);
  }
}

