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
package edu.iu.dsc.tws.examples.compatibility.storm.bench;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class StormBenchmark extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(StormBenchmark.class.getName());
  private static final String PARAM_SIZE = "size";
  private static final String PARAM_PARALLEL_SOURCES = "parallel-sources";
  private static final String PARAM_MESSAGES_COUNT = "messages-count";
  private static final String PARAM_OPERATION = "operation";

  @Override
  public void execute() {
    Integer parallelSources = this.config.getIntegerValue(
        "parallel-sources", 256
    );
    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(
        Config.newBuilder().build());
    Generator generator = new Generator();
    DataSink dataSink = new DataSink();
    taskGraphBuilder.addSource("generator", generator, parallelSources);

    if ("reduce".equals(config.get(PARAM_OPERATION))) {
      taskGraphBuilder.addSink("sink", dataSink)
          .reduce("generator", "edge", Op.SUM, DataType.DOUBLE);
    } else {
      taskGraphBuilder.addSink("sink", dataSink)
          .gather("generator", "edge");
    }

    taskGraphBuilder.setMode(OperationMode.STREAMING);

    DataFlowTaskGraph build = taskGraphBuilder.build();

    this.taskExecutor.execute(build, taskExecutor.plan(build));
  }

  public static class Generator implements ISource {

    private static final long serialVersionUID = -254264903510284798L;

    private TaskContext taskContext;

    private double[] data;

    @Override
    public void execute() {
      taskContext.write("edge", this.data);
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      int dataSize = cfg.getIntegerValue(PARAM_SIZE, 1024);
      LOG.info("Generating data for size " + dataSize);
      int arraySize = dataSize / 8;
      data = new double[arraySize];

      Arrays.fill(data, 1d);

      this.taskContext = context;
    }
  }


  public static class DataSink implements ISink {

    private static final long serialVersionUID = -254264903510284798L;

    private List<Long> timeStamps;
    private int messageCount;
    private boolean done;

    private int dataSize;
    private String operation;
    private int parallelSources;

    @Override
    public boolean execute(IMessage message) {
      if (done) {
        LOG.info("Data collection done. Kill and rerun....");
        return true;
      }
      //double[] data = (double[]) message.getContent();
      //LOG.info("Received message [" + data[0] + "] " + data.length);
      timeStamps.add(System.nanoTime());
      if (timeStamps.size() == messageCount) {
        this.done = true;
        this.dumpData();
      }
      return true;
    }

    private void dumpData() {
      try {
        File file = new File(
            parallelSources + "_" + operation + "_" + dataSize + ".csv"
        );
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
        for (Long timeStamp : this.timeStamps) {
          bufferedWriter.write(timeStamp.toString());
          bufferedWriter.newLine();
        }
        bufferedWriter.close();
        LOG.info("File written to " + file.getAbsolutePath());
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Error in dumping results");
      }
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.messageCount = cfg.getIntegerValue(PARAM_MESSAGES_COUNT, 1000);
      this.dataSize = cfg.getIntegerValue(PARAM_SIZE, -1);
      this.operation = cfg.getStringValue(PARAM_OPERATION);
      this.parallelSources = cfg.getIntegerValue(PARAM_PARALLEL_SOURCES, -1);
      this.timeStamps = new ArrayList<>(this.messageCount);
    }
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(
        Collections.emptyMap()
    );

    JobConfig jobConfig = new JobConfig();


    jobConfig.put(PARAM_SIZE, Integer.valueOf(args[0]));

    int parallelSources = Integer.valueOf(args[1]);
    jobConfig.put(PARAM_PARALLEL_SOURCES, parallelSources);

    jobConfig.put(PARAM_MESSAGES_COUNT, Integer.valueOf(args[2]));

    String operation = "reduce";

    if (args.length == 4) {
      operation = args[3];
    }
    jobConfig.put(PARAM_OPERATION, operation);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("storm-bench-mark");
    jobBuilder.setWorkerClass(StormBenchmark.class.getName());
    jobBuilder.setConfig(jobConfig);
    jobBuilder.addComputeResource(1, 1024, parallelSources + 1);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }

}
