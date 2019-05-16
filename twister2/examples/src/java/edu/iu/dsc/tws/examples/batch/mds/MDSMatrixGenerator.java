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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.RandomStringUtils;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.data.api.DataType;
//import edu.iu.dsc.tws.data.fs.FSDataOutputStream;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.examples.batch.cdfw.CDFConstants;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class MDSMatrixGenerator extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(MDSMatrixGenerator.class.getName());

  @Override
  public void execute() {

    MatrixGeneratorTask generatorTask = new MatrixGeneratorTask();
    MatrixReceiverTask receiverTask = new MatrixReceiverTask();

    TaskGraphBuilder graphBuilder = TaskGraphBuilder.newBuilder(config);
    graphBuilder.addSource("generator", generatorTask, 2);
    ComputeConnection computeConnection = graphBuilder.addSink("receiver", receiverTask,
        2);
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
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;
    private static int dataTypeSize = Short.BYTES;

    private int numberOfRows = 1000;
    private int dimensions = 1000;
    private String outputfile = "/tmp/weight.bin";
    private String directory = "/tmp/matrix";
    private short[] input = new short[numberOfRows * dimensions];

    @Override
    public void execute() {
      try {
        generate();
      } catch (IOException e) {
        e.printStackTrace();
      }
      context.writeEnd(Context.TWISTER2_DIRECT_EDGE, "Matrix Generated");
    }

    private void generate() throws IOException {

      for (int i = 0; i < numberOfRows * dimensions; i++) {
        input[i] = (short) Math.random();
      }
      try {
        ByteBuffer byteBuffer = ByteBuffer.allocate(numberOfRows * dimensions * 2);
        if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
          byteBuffer.order(ByteOrder.BIG_ENDIAN);
        } else {
          byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        }

        byteBuffer.clear();
        ShortBuffer shortOutputBuffer = byteBuffer.asShortBuffer();
        shortOutputBuffer.put(input);

        Path path = new Path(directory);
        FileSystem fs = FileSystem.get(path.toUri(), config);
        if (fs.exists(path)) {
          fs.delete(path, true);
        }
//        FSDataOutputStream outputStream = fs.create(new Path(
//            directory, generateRandom(10) + ".bin"));
//        PrintWriter pw = new PrintWriter(outputStream);
//        pw.print(byteBuffer);
//        outputStream.sync();
//        pw.close();

        FileChannel out = new FileOutputStream(outputfile).getChannel();
        out.write(byteBuffer);
        out.close();
      } catch (IOException e) {
        throw new RuntimeException("IOException Occured");
      }
    }

    private static String generateRandom(int length) {
      boolean useLetters = true;
      boolean useNumbers = false;
      return RandomStringUtils.random(length, useLetters, useNumbers);
    }
  }


  private static class MatrixReceiverTask extends BaseSink {
    private static final long serialVersionUID = -254264120110286748L;

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

    options.addOption(CDFConstants.ARGS_NUMBER_OF_ROWS, true, "4");
    options.addOption(CDFConstants.ARGS_NUMBER_OF_DIMENSIONS, true, "4");

    options.addOption(CDFConstants.ARGS_OUTPUT_FILE, true, "output");
    options.addOption(CDFConstants.ARGS_BYTE_STORAGE_TYPE, true, "bytestorage");

    @SuppressWarnings("deprecation")
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine = commandLineParser.parse(options, args);

    int workers = Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_WORKERS));
    int parallelismValue =
        Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_PARALLELISM_VALUE));

    int rows = Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_NUMBER_OF_ROWS));
    int dimensions =
        Integer.parseInt(commandLine.getOptionValue(CDFConstants.ARGS_NUMBER_OF_DIMENSIONS));

    String output = String.valueOf(commandLine.getOptionValue(CDFConstants.ARGS_OUTPUT_FILE));
    String byteStorage = String.valueOf(
        commandLine.getOptionValue(CDFConstants.ARGS_BYTE_STORAGE_TYPE));

    configurations.put(CDFConstants.ARGS_WORKERS, Integer.toString(workers));
    configurations.put(CDFConstants.ARGS_PARALLELISM_VALUE, Integer.toString(parallelismValue));

    configurations.put(CDFConstants.ARGS_NUMBER_OF_ROWS, Integer.toString(rows));
    configurations.put(CDFConstants.ARGS_NUMBER_OF_DIMENSIONS, Integer.toString(dimensions));

    configurations.put(CDFConstants.ARGS_WORKERS, output);
    configurations.put(CDFConstants.ARGS_BYTE_STORAGE_TYPE, byteStorage);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("MatrixGenerator-job");
    jobBuilder.setWorkerClass(MDSMatrixGenerator.class.getName());
    jobBuilder.addComputeResource(2, 512, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
