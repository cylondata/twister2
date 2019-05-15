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
package edu.iu.dsc.tws.examples.task;

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
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.comms.Constants;
import edu.iu.dsc.tws.examples.task.batch.BTAllGatherExample;
import edu.iu.dsc.tws.examples.task.batch.BTAllReduceExample;
import edu.iu.dsc.tws.examples.task.batch.BTBroadCastExample;
import edu.iu.dsc.tws.examples.task.batch.BTGatherExample;
import edu.iu.dsc.tws.examples.task.batch.BTKeyedGatherExample;
import edu.iu.dsc.tws.examples.task.batch.BTKeyedReduceExample;
import edu.iu.dsc.tws.examples.task.batch.BTPartitionExample;
import edu.iu.dsc.tws.examples.task.batch.BTPartitionKeyedExample;
import edu.iu.dsc.tws.examples.task.batch.BTReduceExample;
import edu.iu.dsc.tws.examples.task.streaming.STAllGatherExample;
import edu.iu.dsc.tws.examples.task.streaming.STAllReduceExample;
import edu.iu.dsc.tws.examples.task.streaming.STBroadCastExample;
import edu.iu.dsc.tws.examples.task.streaming.windowing.STWindowCustomExample;
import edu.iu.dsc.tws.examples.task.streaming.windowing.STWindowExample;
import edu.iu.dsc.tws.examples.task.streaming.STGatherExample;
import edu.iu.dsc.tws.examples.task.streaming.STKeyedGatherExample;
import edu.iu.dsc.tws.examples.task.streaming.STKeyedReduceExample;
import edu.iu.dsc.tws.examples.task.streaming.STPartitionExample;
import edu.iu.dsc.tws.examples.task.streaming.STPartitionKeyedExample;
import edu.iu.dsc.tws.examples.task.streaming.STReduceExample;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkMetadata;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;


public class ExampleTaskMain {
  private static final Logger LOG = Logger.getLogger(ExampleTaskMain.class.getName());

  public ExampleTaskMain() {

  }

  public static void main(String[] args) throws ParseException {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    Options options = new Options();
    options.addOption(Constants.ARGS_WORKERS, true, "Workers");
    options.addOption(Constants.ARGS_SIZE, true, "Size");
    options.addOption(Constants.ARGS_ITR, true, "Iteration");
    options.addOption(Utils.createOption(Constants.ARGS_OPERATION, true, "Operation", true));
    options.addOption(Constants.ARGS_STREAM, false, "Stream");
    options.addOption(Constants.ARGS_WINDOW, false, "WindowType");
    options.addOption(Utils.createOption(Constants.ARGS_TASK_STAGES, true,
        "Number of parallel instances of tasks", true));
    options.addOption(Utils.createOption(Constants.ARGS_GAP, true, "Gap", false));
    options.addOption(Utils.createOption(Constants.ARGS_FNAME, true, "File name", false));
    options.addOption(Utils.createOption(Constants.ARGS_OUTSTANDING, true, "Throughput no of messages", false));
    options.addOption(Utils.createOption(Constants.ARGS_THREADS, true, "Threads", false));
    options.addOption(Utils.createOption(Constants.ARGS_PRINT_INTERVAL, true, "Threads", false));
    options.addOption(Utils.createOption(Constants.ARGS_DATA_TYPE, true, "Data", false));
    options.addOption(Utils.createOption(Constants.ARGS_INIT_ITERATIONS, true, "Data", false));
    options.addOption(Constants.ARGS_VERIFY, false, "verify");
    options.addOption(Utils.createOption(BenchmarkMetadata.ARG_BENCHMARK_METADATA, true, "Benchmark Metadata", false));

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);
    int workers = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_WORKERS));
    int size = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_SIZE));
    int itr = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_ITR));
    String operation = cmd.getOptionValue(Constants.ARGS_OPERATION);
    boolean stream = cmd.hasOption(Constants.ARGS_STREAM);
    boolean window = cmd.hasOption(Constants.ARGS_WINDOW);
    boolean verify = cmd.hasOption(Constants.ARGS_VERIFY);

    String threads = "true";
    if (cmd.hasOption(Constants.ARGS_THREADS)) {
      threads = cmd.getOptionValue(Constants.ARGS_THREADS);
    }

    String taskStages = cmd.getOptionValue(Constants.ARGS_TASK_STAGES);
    String gap = "0";
    if (cmd.hasOption(Constants.ARGS_GAP)) {
      gap = cmd.getOptionValue(Constants.ARGS_GAP);
    }

    String fName = "";
    if (cmd.hasOption(Constants.ARGS_FNAME)) {
      fName = cmd.getOptionValue(Constants.ARGS_FNAME);
    }

    String outstanding = "0";
    if (cmd.hasOption(Constants.ARGS_OUTSTANDING)) {
      outstanding = cmd.getOptionValue(Constants.ARGS_OUTSTANDING);
    }

    String printInt = "1";
    if (cmd.hasOption(Constants.ARGS_PRINT_INTERVAL)) {
      printInt = cmd.getOptionValue(Constants.ARGS_PRINT_INTERVAL);
    }

    String dataType = "default";
    if (cmd.hasOption(Constants.ARGS_DATA_TYPE)) {
      dataType = cmd.getOptionValue(Constants.ARGS_DATA_TYPE);
    }
    String intItr = "0";
    if (cmd.hasOption(Constants.ARGS_INIT_ITERATIONS)) {
      intItr = cmd.getOptionValue(Constants.ARGS_INIT_ITERATIONS);
    }

    boolean runBenchmark = cmd.hasOption(BenchmarkMetadata.ARG_BENCHMARK_METADATA);
    String benchmarkMetadata = null;
    if (runBenchmark) {
      benchmarkMetadata = cmd.getOptionValue(BenchmarkMetadata.ARG_BENCHMARK_METADATA);
    }

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put(Constants.ARGS_ITR, Integer.toString(itr));
    jobConfig.put(Constants.ARGS_OPERATION, operation);
    jobConfig.put(Constants.ARGS_SIZE, Integer.toString(size));
    jobConfig.put(Constants.ARGS_WORKERS, Integer.toString(workers));
    jobConfig.put(Constants.ARGS_TASK_STAGES, taskStages);
    jobConfig.put(Constants.ARGS_GAP, gap);
    jobConfig.put(Constants.ARGS_FNAME, fName);
    jobConfig.put(Constants.ARGS_OUTSTANDING, outstanding);
    jobConfig.put(Constants.ARGS_THREADS, threads);
    jobConfig.put(Constants.ARGS_PRINT_INTERVAL, printInt);
    jobConfig.put(Constants.ARGS_DATA_TYPE, dataType);
    jobConfig.put(Constants.ARGS_INIT_ITERATIONS, intItr);
    jobConfig.put(Constants.ARGS_VERIFY, verify);
    jobConfig.put(Constants.ARGS_STREAM, stream);
    jobConfig.put(Constants.ARGS_WINDOW, window);
    jobConfig.put(BenchmarkMetadata.ARG_RUN_BENCHMARK, runBenchmark);
    if (runBenchmark) {
      jobConfig.put(BenchmarkMetadata.ARG_BENCHMARK_METADATA, benchmarkMetadata);
    }

    // build the job
    if (!stream) {
      switch (operation) {
        case "reduce":
          submitJob(config, workers, jobConfig, BTReduceExample.class.getName());
          break;
        case "allreduce":
          submitJob(config, workers, jobConfig, BTAllReduceExample.class.getName());
          break;
        case "gather":
          submitJob(config, workers, jobConfig, BTGatherExample.class.getName());
          break;
        case "allgather":
          submitJob(config, workers, jobConfig, BTAllGatherExample.class.getName());
          break;
        case "bcast":
          submitJob(config, workers, jobConfig, BTBroadCastExample.class.getName());
          break;
        case "partition":
          submitJob(config, workers, jobConfig, BTPartitionExample.class.getName());
          break;
        case "keyed-reduce":
          submitJob(config, workers, jobConfig, BTKeyedReduceExample.class.getName());
          break;
        case "keyed-gather":
          submitJob(config, workers, jobConfig, BTKeyedGatherExample.class.getName());
          break;
        case "keyed-partition":
          submitJob(config, workers, jobConfig, BTPartitionKeyedExample.class.getName());
          break;
      }
    } else {
      switch (operation) {
        case "direct":
          submitJob(config, workers, jobConfig, STWindowExample.class.getName());
          break;
        case "cdirect":
          submitJob(config, workers, jobConfig, STWindowCustomExample.class.getName());
          break;
        case "reduce":
          submitJob(config, workers, jobConfig, STReduceExample.class.getName());
          break;
        case "allreduce":
          submitJob(config, workers, jobConfig, STAllReduceExample.class.getName());
          break;
        case "gather":
          submitJob(config, workers, jobConfig, STGatherExample.class.getName());
          break;
        case "allgather":
          submitJob(config, workers, jobConfig, STAllGatherExample.class.getName());
          break;
        case "bcast":
          submitJob(config, workers, jobConfig, STBroadCastExample.class.getName());
          break;
        case "partition":
          submitJob(config, workers, jobConfig, STPartitionExample.class.getName());
          break;
        case "keyed-reduce":
          submitJob(config, workers, jobConfig, STKeyedReduceExample.class.getName());
          break;
        case "keyed-gather":
          submitJob(config, workers, jobConfig, STKeyedGatherExample.class.getName());
          break;
        case "keyed-partition":
          submitJob(config, workers, jobConfig, STPartitionKeyedExample.class.getName());
          break;
      }
    }
  }


  private static void submitJob(Config config, int containers, JobConfig jobConfig, String clazz) {
    LOG.info("Submitting Job ...");

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(clazz)
        .setWorkerClass(clazz)
        .addComputeResource(1, 512, containers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }


}
