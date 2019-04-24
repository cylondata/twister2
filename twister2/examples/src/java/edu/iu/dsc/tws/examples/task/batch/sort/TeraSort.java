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
import java.util.concurrent.atomic.AtomicInteger;
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
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkResultsRecorder;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
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
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkMetadata.ARG_BENCHMARK_METADATA;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkMetadata.ARG_RUN_BENCHMARK;

public class TeraSort extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(TeraSort.class.getName());

  private static final String ARG_SIZE = "size";
  private static final String ARG_KEY_SIZE = "keySize";
  private static final String ARG_VALUE_SIZE = "valueSize";

  private static final String ARG_RESOURCE_CPU = "instanceCPUs";
  private static final String ARG_RESOURCE_MEMORY = "instanceMemory";
  private static final String ARG_RESOURCE_INSTANCES = "instances";

  private static final String ARG_TASKS_SOURCES = "sources";
  private static final String ARG_TASKS_SINKS = "sinks";

  private static final String ARG_TUNE_MAX_BYTES_IN_MEMORY = "memoryBytesLimit";
  private static final String ARG_TUNE_MAX_RECORDS_IN_MEMORY = "memoryRecordsLimit";

  private static final String TASK_SOURCE = "int-source";
  private static final String TASK_RECV = "int-recv";
  private static final String EDGE = "edge";

  private static BenchmarkResultsRecorder resultsRecorder;

  private static volatile AtomicInteger tasksCount = new AtomicInteger();

  @Override
  public void execute() {
    resultsRecorder = new BenchmarkResultsRecorder(config, workerId == 0);
    Timing.setDefaultTimingUnit(TimingUnit.MILLI_SECONDS);
    TaskGraphBuilder tgb = TaskGraphBuilder.newBuilder(config);
    tgb.setMode(OperationMode.BATCH);

    IntegerSource integerSource = new IntegerSource();
    tgb.addSource(TASK_SOURCE, integerSource, config.getIntegerValue(ARG_TASKS_SOURCES, 4));

    Receiver receiver = new Receiver();
    tgb.addSink(TASK_RECV, receiver, config.getIntegerValue(ARG_TASKS_SINKS, 4))
        .keyedGather(TASK_SOURCE, EDGE,
            DataType.BYTE_ARRAY, DataType.BYTE_ARRAY, true,
            ByteArrayComparator.getInstance());


    DataFlowTaskGraph dataFlowTaskGraph = tgb.build();
    ExecutionPlan executionPlan = taskExecutor.plan(dataFlowTaskGraph);
    taskExecutor.execute(dataFlowTaskGraph, executionPlan);
    LOG.info("Stopping execution...");
  }

  /**
   * Extracted from hbase source code
   */
  public static final class ByteArrayComparator implements Comparator<byte[]> {

    private static final ByteArrayComparator INSTANCE = new ByteArrayComparator();

    private ByteArrayComparator() {
    }

    public static ByteArrayComparator getInstance() {
      return INSTANCE;
    }

    @Override
    public int compare(byte[] left, byte[] right) {
      for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
        int a = left[i] & 0xff;
        int b = right[j] & 0xff;
        if (a != b) {
          return a - b;
        }
      }
      return left.length - right.length;
    }
  }

  public static class Receiver extends KeyedGatherCompute<byte[], byte[]> implements ISink {

    private boolean timingCondition = false;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      tasksCount.incrementAndGet();
      int lowestTaskIndex = ctx.getTasksByName(TASK_SOURCE).stream()
          .map(TaskInstancePlan::getTaskIndex)
          .min(Comparator.comparingInt(o -> (Integer) o)).get();
      this.timingCondition = ctx.getWorkerId() == 0 && ctx.taskIndex() == lowestTaskIndex;
    }

    @Override
    public boolean keyedGather(Iterator<Tuple<byte[], byte[]>> content) {
      Timing.mark(BenchmarkConstants.TIMING_ALL_RECV, this.timingCondition);
      BenchmarkUtils.markTotalTime(resultsRecorder, this.timingCondition);
      resultsRecorder.writeToCSV();

      byte[] previousKey = null;
      boolean allOrdered = true;
      long tupleCount = 0;
      while (content.hasNext()) {
        Tuple<byte[], byte[]> nextTuple = content.next();
        if (previousKey != null
            && ByteArrayComparator.INSTANCE.compare(previousKey, nextTuple.getKey()) > 0) {
          LOG.info("Unordered tuple found");
          allOrdered = false;
        }
        tupleCount++;
        previousKey = nextTuple.getKey();
      }
      LOG.info(String.format("Received %d tuples. Ordered : %b", tupleCount, allOrdered));
      tasksCount.decrementAndGet();
      return true;
    }
  }

  public static class IntegerSource extends BaseSource {

    private long toSend;
    private long sent;
    private byte[] value;
    private Random random;
    private int keySize;

    private boolean timingCondition = false;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      tasksCount.incrementAndGet();
      int valueSize = cfg.getIntegerValue(ARG_VALUE_SIZE, 90);
      this.keySize = cfg.getIntegerValue(ARG_KEY_SIZE, 10);

      int noOfSources = cfg.getIntegerValue(ARG_TASKS_SOURCES, 4);

      int totalSize = valueSize + keySize;
      this.toSend = (long) (cfg.getDoubleValue(
                ARG_SIZE, 1.0
            ) * 1024 * 1024 * 1024 / totalSize / noOfSources);

      this.value = new byte[valueSize];
      Arrays.fill(this.value, (byte) 1);
      this.random = new Random();

      //time only in the worker0's lowest task
      int lowestTaskIndex = ctx.getTasksByName(TASK_SOURCE).stream()
          .map(TaskInstancePlan::getTaskIndex)
          .min(Comparator.comparingInt(o -> (Integer) o)).get();

      if (ctx.taskIndex() == lowestTaskIndex) {
        LOG.info(String.format("Each source will send %d "
            + "messages of size %d bytes", this.toSend, totalSize));
      }

      timingCondition = ctx.getWorkerId() == 0 && ctx.taskIndex() == lowestTaskIndex;
    }

    @Override
    public void execute() {
      if (sent == 0) {
        LOG.info(String.format("Sending %d messages", this.toSend));
        Timing.mark(BenchmarkConstants.TIMING_ALL_SEND, this.timingCondition);
      }
      byte[] randomKey = new byte[this.keySize];
      this.random.nextBytes(randomKey);
      context.write(EDGE, randomKey, this.value);
      sent++;

      if (sent == toSend) {
        context.end(EDGE);
        LOG.info("Done Sending");
        tasksCount.decrementAndGet();
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
    options.addOption(createOption(ARG_KEY_SIZE, true,
        "Size of the key in bytes of a single Tuple", true));
    options.addOption(createOption(ARG_VALUE_SIZE, true,
        "Size of the value in bytes of a single Tuple", true));

    //resources
    options.addOption(createOption(ARG_RESOURCE_CPU, true,
        "Amount of CPUs to allocate per instance", true));
    options.addOption(createOption(ARG_RESOURCE_MEMORY, true,
        "Amount of Memory in mega bytes to allocate per instance", true));
    options.addOption(createOption(ARG_RESOURCE_INSTANCES, true,
        "No. of instances", true));

    //tasks and sources counts
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

    options.addOption(createOption(
        ARG_BENCHMARK_METADATA, true,
        "Auto generated argument by benchmark suite",
        false
    ));

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);


    jobConfig.put(ARG_SIZE, Double.valueOf(cmd.getOptionValue(ARG_SIZE)));
    jobConfig.put(ARG_VALUE_SIZE, Integer.valueOf(cmd.getOptionValue(ARG_VALUE_SIZE)));
    jobConfig.put(ARG_KEY_SIZE, Integer.valueOf(cmd.getOptionValue(ARG_KEY_SIZE)));

    jobConfig.put(ARG_TASKS_SOURCES, Integer.valueOf(cmd.getOptionValue(ARG_TASKS_SOURCES)));
    jobConfig.put(ARG_TASKS_SINKS, Integer.valueOf(cmd.getOptionValue(ARG_TASKS_SINKS)));

    jobConfig.put(ARG_RESOURCE_INSTANCES,
        Integer.valueOf(cmd.getOptionValue(ARG_RESOURCE_INSTANCES)));

    if (cmd.hasOption(ARG_TUNE_MAX_BYTES_IN_MEMORY)) {
      jobConfig.put(SHUFFLE_MAX_BYTES_IN_MEMORY,
          Integer.valueOf(cmd.getOptionValue(ARG_TUNE_MAX_BYTES_IN_MEMORY)));
    }

    if (cmd.hasOption(ARG_TUNE_MAX_RECORDS_IN_MEMORY)) {
      jobConfig.put(SHUFFLE_MAX_RECORDS_IN_MEMORY,
          Integer.valueOf(cmd.getOptionValue(ARG_TUNE_MAX_RECORDS_IN_MEMORY)));
    }

    if (cmd.hasOption(ARG_BENCHMARK_METADATA)) {
      jobConfig.put(ARG_BENCHMARK_METADATA,
          cmd.getOptionValue(ARG_BENCHMARK_METADATA));
      jobConfig.put(ARG_RUN_BENCHMARK, true);
    }

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(TeraSort.class.getName())
        .setWorkerClass(TeraSort.class.getName())
        .addComputeResource(
            Integer.valueOf(cmd.getOptionValue(ARG_RESOURCE_CPU)),
            Integer.valueOf(cmd.getOptionValue(ARG_RESOURCE_MEMORY)),
            Integer.valueOf(cmd.getOptionValue(ARG_RESOURCE_INSTANCES))
        )
        .setConfig(jobConfig)
        .build();
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
