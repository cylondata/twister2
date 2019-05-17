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
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class MDSWorker extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(MDSWorker.class.getName());

  @Override
  public void execute() {

    MDSWorkerParameters mdsWorkerParameters = MDSWorkerParameters.build(config);

    int parallel = mdsWorkerParameters.getParallelismValue();
    int dimension = mdsWorkerParameters.getDimension();
    int matrixSize = mdsWorkerParameters.getDsize();

    String directory = config.getStringValue("dinput");
    String byteType = config.getStringValue("byteType");

    /** Generate the Matrix for the MDS **/
    MatrixGenerator matrixGen = new MatrixGenerator(config, workerId);
    matrixGen.generate(matrixSize, dimension, directory, byteType);

    /** Task Graph to partition the generated matrix for MDS **/
    MDSDataObjectSource mdsDataObjectSource = new MDSDataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        directory);
    MDSDataObjectSink mdsDataObjectSink = new MDSDataObjectSink();

    TaskGraphBuilder dataObjectGraphBuilder = TaskGraphBuilder.newBuilder(config);
    dataObjectGraphBuilder.addSource("dataobjectsource", mdsDataObjectSource, parallel);

    ComputeConnection dataObjectComputeConnection = dataObjectGraphBuilder.addSink(
        "dataobjectsink", mdsDataObjectSink, parallel);
    dataObjectComputeConnection.direct(
        "dataobjectsource", Context.TWISTER2_DIRECT_EDGE, DataType.OBJECT);
    dataObjectGraphBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph dataObjectTaskGraph = dataObjectGraphBuilder.build();
    //Get the execution plan for the first task graph
    ExecutionPlan dataObjectExecutionPlan = taskExecutor.plan(dataObjectTaskGraph);

    //Actual execution for the first taskgraph
    taskExecutor.execute(dataObjectTaskGraph, dataObjectExecutionPlan);

    /** Task Graph to run the MDS **/
    MatrixGeneratorTask generatorTask = new MatrixGeneratorTask();
    MatrixReceiverTask receiverTask = new MatrixReceiverTask();

    TaskGraphBuilder graphBuilder = TaskGraphBuilder.newBuilder(config);
    graphBuilder.addSource("generator", generatorTask, parallel);
    ComputeConnection computeConnection = graphBuilder.addSink("receiver", receiverTask,
        parallel);
    computeConnection.direct("generator", Context.TWISTER2_DIRECT_EDGE, DataType.OBJECT);
    graphBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph dataFlowTaskGraph = graphBuilder.build();

    //Get the execution plan for the first task graph
    ExecutionPlan executionPlan = taskExecutor.plan(dataFlowTaskGraph);

    //Actual execution for the first taskgraph
    taskExecutor.execute(dataFlowTaskGraph, executionPlan);

    //Retrieve the output of the first task graph
    //DataObject<Object> dataPointsObject = taskExecutor.getOutput(
    //    dataFlowTaskGraph, executionPlan, "receiver");
  }

  private static class MatrixGeneratorTask extends BaseSource {

    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public void execute() {
      context.writeEnd(Context.TWISTER2_DIRECT_EDGE, "MDS Execution");
    }
  }

  private static class MatrixReceiverTask extends BaseSink {
    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public boolean execute(IMessage content) {
      LOG.info("Received message:" + content.getContent().toString());
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
    options.addOption(DataObjectConstants.WORKERS, true, "Workers");
    options.addOption(DataObjectConstants.PARALLELISM_VALUE, true, "parallelism");

    options.addOption(DataObjectConstants.DSIZE, true, "Size of the matrix rows");
    options.addOption(DataObjectConstants.DIMENSIONS, true, "dimension of the matrix");
    options.addOption(DataObjectConstants.BYTE_TYPE, true, "bytetype");

    options.addOption(Utils.createOption(DataObjectConstants.DINPUT_DIRECTORY,
        true, "Matrix Input Creation directory", true));
    options.addOption(Utils.createOption(DataObjectConstants.FILE_SYSTEM,
        true, "file system", true));

    @SuppressWarnings("deprecation")
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    int workers = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.WORKERS));
    int dsize = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.DSIZE));

    int dimension = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.DIMENSIONS));
    int parallelismValue = Integer.parseInt(cmd.getOptionValue(
        DataObjectConstants.PARALLELISM_VALUE));

    String byteType = cmd.getOptionValue(DataObjectConstants.BYTE_TYPE);
    String dataDirectory = cmd.getOptionValue(DataObjectConstants.DINPUT_DIRECTORY);
    String fileSystem = cmd.getOptionValue(DataObjectConstants.FILE_SYSTEM);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put(DataObjectConstants.WORKERS, Integer.toString(workers));
    jobConfig.put(DataObjectConstants.PARALLELISM_VALUE, Integer.toString(parallelismValue));

    jobConfig.put(DataObjectConstants.DIMENSIONS, Integer.toString(dimension));
    jobConfig.put(DataObjectConstants.DSIZE, Integer.toString(dsize));

    jobConfig.put(DataObjectConstants.BYTE_TYPE, byteType);
    jobConfig.put(DataObjectConstants.DINPUT_DIRECTORY, dataDirectory);
    jobConfig.put(DataObjectConstants.FILE_SYSTEM, fileSystem);

    // build JobConfig
    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("MatrixGenerator-job");
    jobBuilder.setWorkerClass(MDSWorker.class.getName());
    jobBuilder.addComputeResource(2, 512, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
