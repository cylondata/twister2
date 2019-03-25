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
package edu.iu.dsc.tws.examples.task.batch.sort;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.typed.KeyedGatherCompute;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class TeraSort extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(TeraSort.class.getName());

  private static boolean gatherDone = false;

  @Override
  public void execute() {
    TaskGraphBuilder tgb = TaskGraphBuilder.newBuilder(config);
    tgb.setMode(OperationMode.BATCH);

    IntegerSource integerSource = new IntegerSource();
    tgb.addSource("int-source", integerSource, 4);

    Receiver receiver = new Receiver();
    tgb.addSink("int-recv", receiver, 4)
        .keyedGather("int-source", "edge",
            DataType.INTEGER, DataType.BYTE, true,
            Comparator.comparingInt(o -> (Integer) o));

    DataFlowTaskGraph dataFlowTaskGraph = tgb.build();
    ExecutionPlan executionPlan = taskExecutor.plan(dataFlowTaskGraph);
    taskExecutor.execute(dataFlowTaskGraph, executionPlan);
    LOG.info("Stopping execution...");
  }

  public static class Receiver extends KeyedGatherCompute<Integer, byte[]> implements ISink {
    @Override
    public boolean keyedGather(Iterator<Tuple<Integer, byte[]>> content) {
      Integer previousKey = Integer.MIN_VALUE;
      boolean allOrdered = true;
      long tupleCount = 0;
      while (content.hasNext()) {
        Tuple<Integer, byte[]> nextTuple = content.next();
        if (previousKey > nextTuple.getKey()) {
          LOG.info("Unordered tuple found");
          allOrdered = false;
        }
        tupleCount++;
        previousKey = nextTuple.getKey();
      }
      gatherDone = true;
      LOG.info(String.format("Received %d tuples. Ordered : %b", tupleCount, allOrdered));
      return true;
    }
  }

  public static class IntegerSource extends BaseSource {

    private long toSend;
    private long sent;
    private byte[] value;
    private Random random;
    private int range;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      this.toSend = cfg.getLongValue(
          "data-amount-in-gb", 1
      ) * 1024 * 1024 * 10;
      this.value = new byte[cfg.getIntegerValue("value-size-in-byte", 1)];
      Arrays.fill(this.value, (byte) 1);
      this.random = new Random();
      this.range = cfg.getIntegerValue("key-range", 1000);
    }

    @Override
    public void execute() {
      if (sent == 0) {
        LOG.info(String.format("Sending %d messages", this.toSend));
      }


      context.write("edge", this.random.nextInt(this.range), this.value);
      sent++;

      if (sent == toSend) {
        context.end("edge");
        LOG.info("Done Sending");
      }
    }
  }

  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());
    JobConfig jobConfig = new JobConfig();

    jobConfig.put("data-amount-in-gb", 1);
    jobConfig.put("value-size-in-byte", 90);
    jobConfig.put("key-range", 1000000);

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(TeraSort.class.getName())
        .setWorkerClass(TeraSort.class.getName())
        .addComputeResource(1, 512, 4)
        .setConfig(jobConfig)
        .build();
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
