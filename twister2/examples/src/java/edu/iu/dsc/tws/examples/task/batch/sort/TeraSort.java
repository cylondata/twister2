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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.utils.bench.TimingUnit;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;
import edu.iu.dsc.tws.task.api.typed.KeyedGatherCompute;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import static edu.iu.dsc.tws.comms.dfw.DataFlowContext.SHUFFLE_MAX_BYTES_IN_MEMORY;
import static edu.iu.dsc.tws.comms.dfw.DataFlowContext.SHUFFLE_MAX_RECORDS_IN_MEMORY;

public class TeraSort extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(TeraSort.class.getName());

  private static final String ARG_SIZE = "size";
  private static final String ARG_KEY_RANGE = "keyRange";
  private static final String ARG_VALUE_SIZE = "valueSize";

  private static final String ARG_RESOURCE_CPU = "instanceCpus";
  private static final String ARG_RESOURCE_MEMORY = "instanceMemory";
  private static final String ARG_RESOURCE_INSTANCES = "instances";

  private static final String ARG_TASKS_SOURCES = "sources";
  private static final String ARG_TASKS_SINKS = "sinks";

  private static final String ARG_TUNE_MAX_BYTES_IN_MEMORY = "memoryBytes";
  private static final String ARG_TUNE_MAX_RECORDS_IN_MEMORY = "memoryRecords";

  private static final String TASK_SOURCE = "int-source";
  private static final String TASK_RECV = "int-recv";
  private static final String EDGE = "edge";

  private static final String TIMING_START = "START";
  private static final String TIMING_END = "END";

  @Override
  public void execute() {
    Timing.setDefaultTimingUnit(TimingUnit.NANO_SECONDS);
    TaskGraphBuilder tgb = TaskGraphBuilder.newBuilder(config);
    tgb.setMode(OperationMode.BATCH);

    IntegerSource integerSource = new IntegerSource();
    tgb.addSource(TASK_SOURCE, integerSource, config.getIntegerValue(ARG_TASKS_SOURCES, 4));

    Receiver receiver = new Receiver();
    tgb.addSink(TASK_RECV, receiver, config.getIntegerValue(ARG_TASKS_SINKS, 4))
        .keyedGather(TASK_SOURCE, EDGE,
            DataType.INTEGER, DataType.BYTE, true,
            Comparator.comparingInt(o -> (Integer) o));

    DataFlowTaskGraph dataFlowTaskGraph = tgb.build();
    ExecutionPlan executionPlan = taskExecutor.plan(dataFlowTaskGraph);
    taskExecutor.execute(dataFlowTaskGraph, executionPlan);
    LOG.info("Stopping execution...");
  }

  public static class Receiver extends KeyedGatherCompute<Integer, byte[]> implements ISink {

    private boolean timingCondition = false;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      int lowestTaskIndex = ctx.getTasksByName(TASK_SOURCE).stream()
          .map(TaskInstancePlan::getTaskIndex)
          .min(Comparator.comparingInt(o -> (Integer) o)).get();
      this.timingCondition = ctx.getWorkerId() == 0 && ctx.taskIndex() == lowestTaskIndex;
    }

    @Override
    public boolean keyedGather(Iterator<Tuple<Integer, byte[]>> content) {
      Timing.mark(TIMING_END, this.timingCondition);
      Timing.averageDiff(TIMING_START, TIMING_END, this.timingCondition);

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

    private boolean timingCondition = false;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      int valueSize = cfg.getIntegerValue(ARG_VALUE_SIZE, 100);
      int totalSize = valueSize + Integer.BYTES;
      this.toSend = cfg.getLongValue(
          ARG_SIZE, 1
      ) * 1024 * 1024 * 1024 / totalSize;
      this.value = new byte[valueSize];
      Arrays.fill(this.value, (byte) 1);
      this.random = new Random();
      this.range = cfg.getIntegerValue(ARG_KEY_RANGE, 1000);

      //time only in the worker0's lowest task
      int lowestTaskIndex = ctx.getTasksByName(TASK_SOURCE).stream()
          .map(TaskInstancePlan::getTaskIndex)
          .min(Comparator.comparingInt(o -> (Integer) o)).get();

      timingCondition = ctx.getWorkerId() == 0 && ctx.taskIndex() == lowestTaskIndex;
    }

    @Override
    public void execute() {
      if (sent == 0) {
        LOG.info(String.format("Sending %d messages", this.toSend));
        Timing.mark(TIMING_START, this.timingCondition);
      }

      context.write(EDGE, this.random.nextInt(this.range), this.value);
      sent++;

      if (sent == toSend) {
        context.end(EDGE);
        LOG.info("Done Sending");
      }
    }
  }

  private static Option createOption(String opt, boolean hasArg,
                                     String description, boolean mandatory) {
    Option option = new Option(opt, hasArg, description);
    option.setRequired(mandatory);
    return option;
  }

  public static void main(String[] args) throws ParseException {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());
    JobConfig jobConfig = new JobConfig();

    Options options = new Options();
    //mandatory configurations
    options.addOption(createOption(ARG_SIZE, true, "Data Size in GigaBytes. "
            + "A source will generate this much of data. Including size of both key and value.",
        true));
    options.addOption(createOption(ARG_KEY_RANGE, true,
        "Range of integer keys. Specify the upped bound for keys. "
            + "0 will always be the lowe bound", true));
    options.addOption(createOption(ARG_VALUE_SIZE, true,
        "Size of the value in bytes of a single Tuple", true));

    //resources
    options.addOption(createOption(ARG_RESOURCE_CPU, true,
        "Amount of CPUs to allocate per instance", true));
    options.addOption(createOption(ARG_RESOURCE_MEMORY, true,
        "Amount of Memory in mega bytes to allocate per instance", true));
    options.addOption(createOption(ARG_RESOURCE_INSTANCES, true,
        "No. of instances", true));

    //tasks
    options.addOption(createOption(ARG_TASKS_SOURCES, true,
        "No of source tasks", true));
    options.addOption(createOption(ARG_TASKS_SINKS, true,
        "No of sink tasks", true));

    //optional configurations (tune performance)
    options.addOption(createOption(
        ARG_TUNE_MAX_BYTES_IN_MEMORY, true, "Maximum bytes to keep in memory",
        false
    ));
    options.addOption(createOption(
        ARG_TUNE_MAX_RECORDS_IN_MEMORY, true, "Maximum records to keep in memory",
        false
    ));

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);


    jobConfig.put(ARG_SIZE, Integer.valueOf(cmd.getOptionValue(ARG_SIZE)));
    jobConfig.put(ARG_VALUE_SIZE, Integer.valueOf(cmd.getOptionValue(ARG_VALUE_SIZE)));
    jobConfig.put(ARG_KEY_RANGE, Integer.valueOf(cmd.getOptionValue(ARG_KEY_RANGE)));

    jobConfig.put(ARG_TASKS_SOURCES, Integer.valueOf(cmd.getOptionValue(ARG_TASKS_SOURCES)));
    jobConfig.put(ARG_TASKS_SINKS, Integer.valueOf(cmd.getOptionValue(ARG_TASKS_SINKS)));

    if (cmd.hasOption(ARG_TUNE_MAX_BYTES_IN_MEMORY)) {
      jobConfig.put(SHUFFLE_MAX_BYTES_IN_MEMORY,
          Integer.valueOf(cmd.getOptionValue(ARG_TUNE_MAX_BYTES_IN_MEMORY)));
    }

    if (cmd.hasOption(ARG_TUNE_MAX_RECORDS_IN_MEMORY)) {
      jobConfig.put(SHUFFLE_MAX_RECORDS_IN_MEMORY,
          Integer.valueOf(cmd.getOptionValue(ARG_TUNE_MAX_RECORDS_IN_MEMORY)));
    }

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
