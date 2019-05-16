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
package edu.iu.dsc.tws.examples.batch.mds;

import java.util.HashMap;
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
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.batch.cdfw.CDFConstants;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public final class MDSMatrixGenerator {

  private static final Logger LOG = Logger.getLogger(MDSMatrixGenerator.class.getName());

  private MDSMatrixGenerator() {
  }

  public static class MDSMatrixGeneratorDriver extends BaseDriver {

    @Override
    public void execute(CDFWEnv exec) {
      DafaFlowJobConfig dafaFlowJobConfig = new DafaFlowJobConfig();
      MatrixGeneratorTask generatorTask = new MatrixGeneratorTask();
      MatrixReceivingTask receivingTask = new MatrixReceivingTask();

      TaskGraphBuilder graphBuilderX = TaskGraphBuilder.newBuilder(exec.getConfig());
      graphBuilderX.addSource("generator", generatorTask, 4);
      ComputeConnection computeConnection = graphBuilderX.addSink("receiver", receivingTask,
          1);
      computeConnection.direct("generator", Context.TWISTER2_DIRECT_EDGE, DataType.OBJECT);

      graphBuilderX.setMode(OperationMode.BATCH);
      DataFlowTaskGraph batchGraph = graphBuilderX.build();

      DataFlowGraph job = DataFlowGraph.newSubGraphJob("matrixgenerator", batchGraph).
          setWorkers(4).addDataFlowJobConfig(dafaFlowJobConfig);
      exec.executeDataFlowGraph(job);
    }
  }

  private static class MatrixGeneratorTask extends BaseSource {

    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public void execute() {
      context.writeEnd(Context.TWISTER2_DIRECT_EDGE, "hello");
    }
  }

  private static class MatrixReceivingTask extends BaseSink {

    @Override
    public boolean execute(IMessage content) {
      LOG.info("Received message:" + content.getContent());
      return false;
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
        .setJobName(MDSMatrixGenerator.class.getName())
        .setDriverClass(MDSMatrixGeneratorDriver.class.getName())
        .addComputeResource(1, 512, instances)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
