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
package edu.iu.dsc.tws.examples.batch.sortop;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.comms.batch.BKeyedGather;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import static edu.iu.dsc.tws.api.comms.CommunicationContext.SHUFFLE_MAX_BYTES_IN_MEMORY;
import static edu.iu.dsc.tws.api.comms.CommunicationContext.SHUFFLE_MAX_FILE_SIZE;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkMetadata.ARG_BENCHMARK_METADATA;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkMetadata.ARG_RUN_BENCHMARK;

public class SortJob implements IWorker {
  private static final Logger LOG = Logger.getLogger(SortJob.class.getName());

  public static final String ARG_INPUT_FILE = "inputFile";
  public static final String ARG_OUTPUT_FOLDER = "outputFolder";

  public static final String ARG_SIZE = "size";
  public static final String ARG_KEY_SIZE = "keySize";
  public static final String ARG_VALUE_SIZE = "valueSize";
  public static final String ARG_KEY_SEED = "keySeed";

  public static final String ARG_RESOURCE_CPU = "instanceCPUs";
  public static final String ARG_RESOURCE_MEMORY = "instanceMemory";
  public static final String ARG_RESOURCE_INSTANCES = "instances";

  public static final String ARG_TASKS_SOURCES = "sources";
  public static final String ARG_TASKS_SINKS = "sinks";

  private static final String ARG_FIXED_SCHEMA = "fixedSchema";

  public static final String ARG_TUNE_MAX_BYTES_IN_MEMORY = "memoryBytesLimit";
  public static final String ARG_TUNE_MAX_SHUFFLE_FILE_SIZE = "fileSizeBytes";

  private BKeyedGather gather;

  private static final int NO_OF_TASKS = 4;

  private int workerId;

  private Set<Integer> sources;
  private Set<Integer> destinations;
  private LogicalPlan logicalPlan;
  private List<Integer> taskStages = new ArrayList<>();
  private WorkerEnvironment workerEnv;

  @Override
  public void execute(Config cfg, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    this.workerId = workerID;

    // create a worker environment & setup the network
    this.workerEnv = WorkerEnvironment.init(cfg, workerID, workerController, persistentVolume,
        volatileVolume);

    int noOfSources = cfg.getIntegerValue(ARG_TASKS_SOURCES, 4);
    int noOfTargets = cfg.getIntegerValue(ARG_TASKS_SINKS, 4);
    taskStages.add(noOfSources);
    taskStages.add(noOfTargets);

    // lets create the task plan
    this.logicalPlan = Utils.createStageLogicalPlan(workerEnv, taskStages);

    // set up the tasks
    setupTasks(cfg);

    int valueSize = cfg.getIntegerValue(SortJob.ARG_VALUE_SIZE, 90);
    int keySize = cfg.getIntegerValue(SortJob.ARG_KEY_SIZE, 10);

    MessageSchema schema = MessageSchema.noSchema();

    if (cfg.getBooleanValue(ARG_FIXED_SCHEMA, false)) {
      LOG.info("Using fixed schema feature with message size : "
          + (keySize + valueSize) + " and key size : " + keySize);
      schema = MessageSchema.ofSize(keySize + valueSize, keySize);
    }

    gather = new BKeyedGather(workerEnv.getCommunicator(), logicalPlan, sources, destinations,
        MessageTypes.BYTE_ARRAY, MessageTypes.BYTE_ARRAY,
        new RecordSave(), new ByteSelector(), true, new IntegerComparator(), true, schema);

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, logicalPlan, taskStages, 0);
    int thisSource = tasksOfExecutor.iterator().next();
    RecordSource source = new RecordSource(cfg, workerId, gather, thisSource);
    long start = System.currentTimeMillis();
    // run until we send
    source.run();

    // wait until we receive
    progress();
    LOG.info("Time: " + (System.currentTimeMillis() - start));
  }

  private void setupTasks(Config cfg) {
    int noOfSources = cfg.getIntegerValue(SortJob.ARG_TASKS_SOURCES, 4);
    int noOfTargets = cfg.getIntegerValue(SortJob.ARG_TASKS_SINKS, 4);

    sources = new HashSet<>();
    for (int i = 0; i < noOfSources; i++) {
      sources.add(i);
    }
    destinations = new HashSet<>();
    for (int i = 0; i < noOfTargets; i++) {
      destinations.add(noOfSources + i);
    }
    LOG.fine(String.format("%d sources %s destinations %s",
        logicalPlan.getThisExecutor(), sources, destinations));
  }

  private class IntegerComparator implements Comparator<Object> {
    @Override
    public int compare(Object o1, Object o2) {
      byte[] left = (byte[]) o1;
      byte[] right = (byte[]) o2;
      for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
        byte a = left[i];
        byte b = right[j];
        if (a != b) {
          return a - b;
        }
      }
      return left.length - right.length;
    }
  }

  private void progress() {
    // we need to communicationProgress the communication
    while (!gather.isComplete()) {
      gather.progressChannel();
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

    //fixed schema
    options.addOption(createOption(
        ARG_FIXED_SCHEMA, false, "Use fixed schema feature", false
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

    if (cmd.hasOption(ARG_FIXED_SCHEMA)) {
      jobConfig.put(ARG_FIXED_SCHEMA, true);
    }

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(SortJob.class.getName())
        .setWorkerClass(SortJob.class.getName())
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
