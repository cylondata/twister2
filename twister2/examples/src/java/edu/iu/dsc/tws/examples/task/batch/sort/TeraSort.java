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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
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
import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkResultsRecorder;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.utils.bench.TimingUnit;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.TaskPartitioner;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;
import edu.iu.dsc.tws.task.api.typed.AllReduceCompute;
import edu.iu.dsc.tws.task.api.typed.batch.BKeyedGatherUnGroupedCompute;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import static edu.iu.dsc.tws.comms.dfw.DataFlowContext.SHUFFLE_MAX_BYTES_IN_MEMORY;
import static edu.iu.dsc.tws.comms.dfw.DataFlowContext.SHUFFLE_MAX_FILE_SIZE;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkMetadata.ARG_BENCHMARK_METADATA;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkMetadata.ARG_RUN_BENCHMARK;

public class TeraSort extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(TeraSort.class.getName());

  private static final String ARG_INPUT_FILE = "inputFile";
  private static final String ARG_OUTPUT_FOLDER = "outputFolder";

  private static final String ARG_SIZE = "size";
  private static final String ARG_KEY_SIZE = "keySize";
  private static final String ARG_VALUE_SIZE = "valueSize";
  private static final String ARG_KEY_SEED = "keySeed";

  private static final String ARG_RESOURCE_CPU = "instanceCPUs";
  private static final String ARG_RESOURCE_MEMORY = "instanceMemory";
  private static final String ARG_RESOURCE_INSTANCES = "instances";

  private static final String ARG_TASKS_SOURCES = "sources";
  private static final String ARG_TASKS_SINKS = "sinks";

  private static final String ARG_TUNE_MAX_BYTES_IN_MEMORY = "memoryBytesLimit";
  private static final String ARG_TUNE_MAX_SHUFFLE_FILE_SIZE = "fileSizeBytes";

  private static final String TASK_SOURCE = "sort-source";
  private static final String TASK_RECV = "sort-recv";
  private static final String TASK_SAMPLER = "sample-source";
  private static final String TASK_SAMPLER_REDUCE = "sample-recv";
  private static final String EDGE = "edge";

  private static BenchmarkResultsRecorder resultsRecorder;

  private static volatile AtomicInteger tasksCount = new AtomicInteger();

  @Override
  public void execute() {
    resultsRecorder = new BenchmarkResultsRecorder(config, workerId == 0);
    Timing.setDefaultTimingUnit(TimingUnit.MILLI_SECONDS);

    String filePath = config.getStringValue(ARG_INPUT_FILE, null);

    //Sampling Graph : if file based only
    TaskPartitioner taskPartitioner;
    if (filePath != null) {
      TaskGraphBuilder samplingGraph = TaskGraphBuilder.newBuilder(config);
      samplingGraph.setMode(OperationMode.BATCH);

      Sampler samplerTask = new Sampler();
      samplingGraph.addSource(TASK_SAMPLER, samplerTask,
          config.getIntegerValue(ARG_TASKS_SOURCES, 4));

      final int keySize = config.getIntegerValue(ARG_KEY_SIZE, 10);

      SamplerReduce samplerReduce = new SamplerReduce();
      samplingGraph.addCompute(TASK_SAMPLER_REDUCE, samplerReduce,
          config.getIntegerValue(ARG_RESOURCE_INSTANCES, 4))
          .allreduce(TASK_SAMPLER, EDGE, (IFunction) (object1, object2) -> {
            byte[] minMax1 = (byte[]) object1;
            byte[] minMax2 = (byte[]) object2;

            byte[] min1 = Arrays.copyOfRange(minMax1, 0, keySize);
            byte[] max1 = Arrays.copyOfRange(minMax1, keySize, minMax1.length);

            byte[] min2 = Arrays.copyOfRange(minMax2, 0, keySize);
            byte[] max2 = Arrays.copyOfRange(minMax2, keySize, minMax2.length);

            byte[] newMinMax = new byte[keySize * 2];
            byte[] min = min1;
            byte[] max = max1;
            if (ByteArrayComparator.getInstance().compare(min1, min2) > 0) {
              min = min2;
            }

            if (ByteArrayComparator.getInstance().compare(max1, max2) < 0) {
              max = max2;
            }

            System.arraycopy(min, 0, newMinMax, 0, keySize);
            System.arraycopy(max, 0, newMinMax, keySize, keySize);

            return newMinMax;
          });
      DataFlowTaskGraph sampleGraphBuild = samplingGraph.build();
      ExecutionPlan sampleTaskPlan = taskExecutor.plan(sampleGraphBuild);
      taskExecutor.execute(sampleGraphBuild, sampleTaskPlan);
      DataObject<byte[]> output = taskExecutor.getOutput(sampleGraphBuild,
          sampleTaskPlan, TASK_SAMPLER_REDUCE);
      LOG.info("Sample output received");
      taskPartitioner = new TaskPartitionerForSampledData(
          output.getPartitions()[0].getConsumer().next(),
          keySize
      );
    } else {
      taskPartitioner = new TaskPartitionerForRandom();
    }


    // Sort Graph
    TaskGraphBuilder teraSortTaskGraph = TaskGraphBuilder.newBuilder(config);
    teraSortTaskGraph.setMode(OperationMode.BATCH);
    BaseSource dataSource;
    if (filePath == null) {
      dataSource = new RandomDataSource();
    } else {
      dataSource = new FileDataSource();
    }
    teraSortTaskGraph.addSource(TASK_SOURCE, dataSource,
        config.getIntegerValue(ARG_TASKS_SOURCES, 4));

    Receiver receiver = new Receiver();
    teraSortTaskGraph.addSink(TASK_RECV, receiver,
        config.getIntegerValue(ARG_TASKS_SINKS, 4))
        .keyedGather(TASK_SOURCE)
        .viaEdge(EDGE)
        .withDataType(DataType.BYTE_ARRAY)
        .withKeyType(DataType.BYTE_ARRAY)
        .withTaskPartitioner(taskPartitioner)
        .useDisk(true)
        .sortBatchByKey(true, ByteArrayComparator.getInstance())
        .groupBatchByKey(false);


    DataFlowTaskGraph dataFlowTaskGraph = teraSortTaskGraph.build();
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

  public static class SamplerReduce extends AllReduceCompute<byte[]> implements Collector {

    private DataPartition<byte[]> minMax;


    @Override
    public DataPartition<?> get() {
      return minMax;
    }

    @Override
    public boolean allReduce(byte[] content) {
      this.minMax = new EntityPartition<>(0, content);
      return true;
    }
  }

  public static class Sampler extends BaseSource {

    private FileChannel fileChannel;
    private int keySize;
    private int valueSize;
    private int sampleSize;
    private ByteBuffer[] tupleBuffers;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      super.prepare(cfg, ctx);
      String inputFile = cfg.getStringValue(ARG_INPUT_FILE);

      try {
        this.fileChannel = FileChannel.open(Paths.get(String.format(inputFile, ctx.taskIndex())));
      } catch (IOException e) {
        throw new RuntimeException("Error in opening file data source", e);
      }
      this.keySize = cfg.getIntegerValue(ARG_KEY_SIZE, 10);
      this.valueSize = cfg.getIntegerValue(ARG_VALUE_SIZE, 90);
      this.tupleBuffers = new ByteBuffer[]{ByteBuffer.allocate(keySize),
          ByteBuffer.allocate(valueSize)};
      this.sampleSize = 100;
    }

    @Override
    public void execute() {
      try {
        List<byte[]> keys = new ArrayList<>();
        while (this.fileChannel.read(this.tupleBuffers) != -1 && keys.size() < this.sampleSize) {
          byte[] key = new byte[this.keySize];
          byte[] value = new byte[this.valueSize];

          this.tupleBuffers[0].flip();
          this.tupleBuffers[1].flip();

          this.tupleBuffers[0].get(key);
          this.tupleBuffers[1].get(value);

          keys.add(key);

          this.tupleBuffers[0].rewind();
          this.tupleBuffers[1].rewind();
        }
        keys.sort(ByteArrayComparator.getInstance());

        byte[] minMax = new byte[this.keySize * 2];

        System.arraycopy(keys.get(0), 0, minMax, 0, this.keySize);
        System.arraycopy(keys.get(keys.size() - 1), 0, minMax, this.keySize, this.keySize);

        context.writeEnd(EDGE, minMax);
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Error in reading file channel");
      }
    }
  }

  public static class Receiver extends BKeyedGatherUnGroupedCompute<byte[], byte[]>
      implements ISink {

    private boolean timingCondition = false;
    private BufferedOutputStream resultsWriter;
    private int taskIndex = -1;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      tasksCount.incrementAndGet();
      int lowestTaskIndex = ctx.getTasksByName(TASK_SOURCE).stream()
          .map(TaskInstancePlan::getTaskIndex)
          .min(Comparator.comparingInt(o -> (Integer) o)).get();
      this.timingCondition = ctx.getWorkerId() == 0 && ctx.taskIndex() == lowestTaskIndex;
      String outFolder = cfg.getStringValue(ARG_OUTPUT_FOLDER);
      this.taskIndex = ctx.taskIndex();
      if (outFolder != null) {
        try {
          File outFile = new File(outFolder, String.valueOf(ctx.taskIndex()));
          outFile.createNewFile();
          resultsWriter = new BufferedOutputStream(
              new FileOutputStream(outFile)
          );
        } catch (IOException e) {
          LOG.log(Level.WARNING, "Failed to create output file", e);
        }
      }
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

        if (this.resultsWriter != null) {
          try {
            resultsWriter.write(nextTuple.getKey());
            resultsWriter.write(nextTuple.getValue());
          } catch (IOException e) {
            LOG.log(Level.WARNING, "Failed to write results to file.", e);
          }
        }
      }
      LOG.info(String.format("Received %d tuples. Ordered : %b", tupleCount, allOrdered));
      tasksCount.decrementAndGet();
      try {
        if (resultsWriter != null) {
          resultsWriter.close();
        }
      } catch (IOException e) {
        LOG.log(Level.WARNING, "Failed to close file channel of results writer", e);
      }
      return true;
    }
  }

  public static class FileDataSource extends BaseSource {

    private FileChannel fileChannel;
    private ByteBuffer[] tupleBuffers;
    private long sent = 0;
    private boolean timingCondition;
    private int keySize;
    private int valueSize;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      String inputFile = cfg.getStringValue(ARG_INPUT_FILE);

      try {
        this.fileChannel = FileChannel.open(Paths.get(String.format(inputFile, ctx.taskIndex())));
      } catch (IOException e) {
        throw new RuntimeException("Error in opening file data source", e);
      }
      this.keySize = cfg.getIntegerValue(ARG_KEY_SIZE, 10);
      this.valueSize = cfg.getIntegerValue(ARG_VALUE_SIZE, 90);
      this.tupleBuffers = new ByteBuffer[]{ByteBuffer.allocate(keySize),
          ByteBuffer.allocate(valueSize)};

      //time only in the worker0's lowest task
      int lowestTaskIndex = ctx.getTasksByName(TASK_SOURCE).stream()
          .map(TaskInstancePlan::getTaskIndex)
          .min(Comparator.comparingInt(o -> (Integer) o)).get();

      timingCondition = ctx.getWorkerId() == 0 && ctx.taskIndex() == lowestTaskIndex;
    }

    @Override
    public void execute() {
      if (sent == 0) {
        LOG.info("Sending messages from a file data source...");
        Timing.mark(BenchmarkConstants.TIMING_ALL_SEND, this.timingCondition);
      }
      try {
        if (this.fileChannel.read(this.tupleBuffers) != -1) {
          byte[] key = new byte[this.keySize];
          byte[] value = new byte[this.valueSize];

          this.tupleBuffers[0].flip();
          this.tupleBuffers[1].flip();

          this.tupleBuffers[0].get(key);
          this.tupleBuffers[1].get(value);

          context.write(EDGE, key, value);

          this.tupleBuffers[0].rewind();
          this.tupleBuffers[1].rewind();
        } else {
          context.end(EDGE);
          LOG.info("Done Sending");
          tasksCount.decrementAndGet();
        }
      } catch (IOException e) {
        throw new RuntimeException("Error in reading file data source", e);
      } finally {
        sent++;
      }
    }
  }

  public static class RandomDataSource extends BaseSource {

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
      this.random = new Random(cfg.getIntegerValue(ARG_KEY_SEED, 1000));

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

      if (sent == this.toSend) {
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

    //file based mode configuration
    options.addOption(createOption(ARG_INPUT_FILE, true,
        "Path to the file containing input tuples. "
            + "Path can be specified with %d, where it will be replaced by task index. For example,"
            + "input-%d, will be considered as input-0 in source task having index 0.",
        false));

    //non-file based mode configurations
    options.addOption(createOption(ARG_SIZE, true, "Data Size in GigaBytes. "
            + "A source will generate this much of data. Including size of both key and value.",
        false));
    options.addOption(createOption(ARG_KEY_SIZE, true,
        "Size of the key in bytes of a single Tuple", true));
    options.addOption(createOption(ARG_KEY_SEED, true,
        "Size of the key in bytes of a single Tuple", false));
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
        ARG_TUNE_MAX_SHUFFLE_FILE_SIZE, true, "Maximum records to keep in memory",
        false
    ));

    options.addOption(createOption(
        ARG_BENCHMARK_METADATA, true,
        "Auto generated argument by benchmark suite",
        false
    ));

    //output folder
    options.addOption(createOption(
        ARG_OUTPUT_FOLDER, true,
        "Folder to save output files",
        false
    ));

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    if (cmd.hasOption(ARG_INPUT_FILE)) {
      jobConfig.put(ARG_INPUT_FILE, cmd.getOptionValue(ARG_INPUT_FILE));
    } else {
      jobConfig.put(ARG_SIZE, Double.valueOf(cmd.getOptionValue(ARG_SIZE)));
      jobConfig.put(ARG_VALUE_SIZE, Integer.valueOf(cmd.getOptionValue(ARG_VALUE_SIZE)));
      jobConfig.put(ARG_KEY_SIZE, Integer.valueOf(cmd.getOptionValue(ARG_KEY_SIZE)));
    }

    jobConfig.put(ARG_TASKS_SOURCES, Integer.valueOf(cmd.getOptionValue(ARG_TASKS_SOURCES)));
    jobConfig.put(ARG_TASKS_SINKS, Integer.valueOf(cmd.getOptionValue(ARG_TASKS_SINKS)));

    jobConfig.put(ARG_RESOURCE_INSTANCES,
        Integer.valueOf(cmd.getOptionValue(ARG_RESOURCE_INSTANCES)));

    if (cmd.hasOption(ARG_TUNE_MAX_BYTES_IN_MEMORY)) {
      long maxBytesInMemory = Long.valueOf(cmd.getOptionValue(ARG_TUNE_MAX_BYTES_IN_MEMORY));
      jobConfig.put(SHUFFLE_MAX_BYTES_IN_MEMORY, maxBytesInMemory);
      jobConfig.put(ARG_TUNE_MAX_BYTES_IN_MEMORY, maxBytesInMemory); //for benchmark service
    }

    if (cmd.hasOption(ARG_TUNE_MAX_SHUFFLE_FILE_SIZE)) {
      long maxRecordsInMemory = Long.valueOf(cmd.getOptionValue(ARG_TUNE_MAX_SHUFFLE_FILE_SIZE));
      jobConfig.put(SHUFFLE_MAX_FILE_SIZE, maxRecordsInMemory);
      jobConfig.put(ARG_TUNE_MAX_SHUFFLE_FILE_SIZE, maxRecordsInMemory);
    }

    if (cmd.hasOption(ARG_BENCHMARK_METADATA)) {
      jobConfig.put(ARG_BENCHMARK_METADATA,
          cmd.getOptionValue(ARG_BENCHMARK_METADATA));
      jobConfig.put(ARG_RUN_BENCHMARK, true);
    }

    if (cmd.hasOption(ARG_OUTPUT_FOLDER)) {
      jobConfig.put(ARG_OUTPUT_FOLDER, cmd.getOptionValue(ARG_OUTPUT_FOLDER));
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
