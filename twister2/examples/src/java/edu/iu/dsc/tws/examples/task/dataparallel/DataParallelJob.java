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
package edu.iu.dsc.tws.examples.task.dataparallel;

import java.io.IOException;
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
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.comms.Constants;
import edu.iu.dsc.tws.examples.utils.DataGenerator;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

public final class DataParallelJob {
  private static final Logger LOG = Logger.getLogger(DataParallelJob.class.getName());

  private DataParallelJob() {
  }

  public static void main(String[] args) throws ParseException, IOException {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    Options options = new Options();
    options.addOption(Constants.ARGS_WORKERS, true, "Workers");
    options.addOption(Constants.ARGS_SIZE, true, "Size of the file");
    options.addOption(Constants.ARGS_NUMBER_OF_FILES, true, "Number of files");
    options.addOption(Constants.ARGS_SHARED_FILE_SYSTEM, false, "Shared file system");
    options.addOption(Utils.createOption(Constants.ARGS_INPUT_DIRECTORY,
        true, "Input directory", true));
    options.addOption(Utils.createOption(Constants.ARGS_OUTPUT_DIRECTORY,
        true, "Output directory", true));

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);
    int workers = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_WORKERS));
    int size = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_SIZE));
    String fName = cmd.getOptionValue(Constants.ARGS_INPUT_DIRECTORY);
    String outDir = cmd.getOptionValue(Constants.ARGS_OUTPUT_DIRECTORY);
    int numFiles = Integer.parseInt(cmd.getOptionValue(Constants.ARGS_NUMBER_OF_FILES));
    boolean shared = cmd.hasOption(Constants.ARGS_SHARED_FILE_SYSTEM);

    // we we are a shared file system, lets generate data at the client
    if (shared) {
      DataGenerator.generateData("txt", new Path(fName), numFiles, size, 100);
    }

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put(Constants.ARGS_SIZE, Integer.toString(size));
    jobConfig.put(Constants.ARGS_WORKERS, Integer.toString(workers));
    jobConfig.put(Constants.ARGS_INPUT_DIRECTORY, fName);
    jobConfig.put(Constants.ARGS_OUTPUT_DIRECTORY, outDir);
    jobConfig.put(Constants.ARGS_NUMBER_OF_FILES, numFiles);
    jobConfig.put(Constants.ARGS_SHARED_FILE_SYSTEM, shared);

    // build the job
    submitJob(config, workers, jobConfig, DataParallelWorker.class.getName());
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
