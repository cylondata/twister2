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
package edu.iu.dsc.tws.examples.batch.kmeansoptimization;

import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.dataparallelimpl.DataParallelConstants;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

public class KMeansJobMain {

  private static final Logger LOG = Logger.getLogger(KMeansJobMain.class.getName());

  public static void main(String[] args) throws ParseException, IOException {
    LOG.log(Level.INFO, "KMeans Clustering Job");

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    Options options = new Options();
    options.addOption(DataParallelConstants.ARGS_WORKERS, true, "Workers");
    options.addOption(DataParallelConstants.ARGS_CSIZE, true, "Size of the dapoints file");
    options.addOption(DataParallelConstants.ARGS_DSIZE, true, "Size of the centroids file");
    options.addOption(DataParallelConstants.ARGS_NUMBER_OF_FILES, true, "Number of files");
    options.addOption(DataParallelConstants.ARGS_SHARED_FILE_SYSTEM, false, "Shared file system");
    options.addOption(DataParallelConstants.ARGS_DIMENSIONS, true, "dim");
    options.addOption(DataParallelConstants.ARGS_PARALLELISM_VALUE, true, "parallelism");

    options.addOption(Utils.createOption(DataParallelConstants.ARGS_DINPUT_DIRECTORY,
        true, "Data points Input directory", true));
    options.addOption(Utils.createOption(DataParallelConstants.ARGS_CINPUT_DIRECTORY,
        true, "Centroids Input directory", true));
    options.addOption(Utils.createOption(DataParallelConstants.ARGS_OUTPUT_DIRECTORY,
        true, "Output directory", true));

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    int workers = Integer.parseInt(cmd.getOptionValue(DataParallelConstants.ARGS_WORKERS));
    int dsize = Integer.parseInt(cmd.getOptionValue(DataParallelConstants.ARGS_DSIZE));
    int csize = Integer.parseInt(cmd.getOptionValue(DataParallelConstants.ARGS_CSIZE));
    int numFiles = Integer.parseInt(cmd.getOptionValue(DataParallelConstants.ARGS_NUMBER_OF_FILES));
    int dimension = Integer.parseInt(cmd.getOptionValue(DataParallelConstants.ARGS_DIMENSIONS));
    int parallelismValue = Integer.parseInt(cmd.getOptionValue(
        DataParallelConstants.ARGS_PARALLELISM_VALUE));

    String dataDirectory = cmd.getOptionValue(DataParallelConstants.ARGS_DINPUT_DIRECTORY);
    String centroidDirectory = cmd.getOptionValue(DataParallelConstants.ARGS_CINPUT_DIRECTORY);
    String outputDirectory = cmd.getOptionValue(DataParallelConstants.ARGS_OUTPUT_DIRECTORY);

    boolean shared = cmd.hasOption(DataParallelConstants.ARGS_SHARED_FILE_SYSTEM);

    // we we are a shared file system, lets generate data at the client
    /*if (shared) {
      KMeansDataGenerator.generateData(
          "txt", new Path(dataDirectory), numFiles, dsize, 100, dimension);
      KMeansDataGenerator.generateData(
          "txt", new Path(centroidDirectory), numFiles, csize, 100, dimension);
    }*/

    // build JobConfig
    JobConfig jobConfig = new JobConfig();

    jobConfig.put(DataParallelConstants.ARGS_DINPUT_DIRECTORY, dataDirectory);
    jobConfig.put(DataParallelConstants.ARGS_CINPUT_DIRECTORY, centroidDirectory);
    jobConfig.put(DataParallelConstants.ARGS_OUTPUT_DIRECTORY, outputDirectory);

    jobConfig.put(DataParallelConstants.ARGS_DSIZE, Integer.toString(dsize));
    jobConfig.put(DataParallelConstants.ARGS_CSIZE, Integer.toString(csize));
    jobConfig.put(DataParallelConstants.ARGS_WORKERS, Integer.toString(workers));
    jobConfig.put(DataParallelConstants.ARGS_NUMBER_OF_FILES, Integer.toString(numFiles));
    jobConfig.put(DataParallelConstants.ARGS_DIMENSIONS, Integer.toString(dimension));
    jobConfig.put(DataParallelConstants.ARGS_PARALLELISM_VALUE, Integer.toString(parallelismValue));

    jobConfig.put(DataParallelConstants.ARGS_SHARED_FILE_SYSTEM, shared);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("KMeans-job");
    jobBuilder.setWorkerClass(KMeansJob.class.getName());
    jobBuilder.addComputeResource(2, 512, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
