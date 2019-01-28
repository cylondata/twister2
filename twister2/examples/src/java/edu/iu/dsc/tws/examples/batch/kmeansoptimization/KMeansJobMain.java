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
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

public class KMeansJobMain {

  private static final Logger LOG = Logger.getLogger(KMeansJobMain.class.getName());

  public static void main(String[] args) throws ParseException, IOException {
    LOG.log(Level.INFO, "KMeans Clustering Job");

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    Options options = new Options();
    options.addOption(KMeansConstants.ARGS_WORKERS, true, "Workers");
    options.addOption(KMeansConstants.ARGS_CSIZE, true, "Size of the dapoints file");
    options.addOption(KMeansConstants.ARGS_DSIZE, true, "Size of the centroids file");
    options.addOption(KMeansConstants.ARGS_NUMBER_OF_FILES, true, "Number of files");
    options.addOption(KMeansConstants.ARGS_SHARED_FILE_SYSTEM, false, "Shared file system");
    options.addOption(Utils.createOption(KMeansConstants.ARGS_DINPUT_DIRECTORY,
        true, "Data points Input directory", true));
    options.addOption(Utils.createOption(KMeansConstants.ARGS_CINPUT_DIRECTORY,
        true, "Centroids Input directory", true));
    options.addOption(Utils.createOption(KMeansConstants.ARGS_OUTPUT_DIRECTORY,
        true, "Output directory", true));
    options.addOption(KMeansConstants.ARGS_DIMENSIONS, true, "dim");

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    int workers = Integer.parseInt(cmd.getOptionValue(KMeansConstants.ARGS_WORKERS));
    int dsize = Integer.parseInt(cmd.getOptionValue(KMeansConstants.ARGS_DSIZE));
    int csize = Integer.parseInt(cmd.getOptionValue(KMeansConstants.ARGS_CSIZE));
    String dFileName = cmd.getOptionValue(KMeansConstants.ARGS_DINPUT_DIRECTORY);
    String cFileName = cmd.getOptionValue(KMeansConstants.ARGS_CINPUT_DIRECTORY);
    String outDir = cmd.getOptionValue(KMeansConstants.ARGS_OUTPUT_DIRECTORY);
    int numFiles = Integer.parseInt(cmd.getOptionValue(KMeansConstants.ARGS_NUMBER_OF_FILES));
    boolean shared = cmd.hasOption(KMeansConstants.ARGS_SHARED_FILE_SYSTEM);
    int dimension = Integer.parseInt(cmd.getOptionValue(KMeansConstants.ARGS_DIMENSIONS));

    // we we are a shared file system, lets generate data at the client
    if (shared) {
      KMeansDataGenerator.generateData(
          "txt", new Path(dFileName), numFiles, dsize, 100, dimension);
      KMeansDataGenerator.generateData(
          "txt", new Path(cFileName), numFiles, csize, 100, dimension);
    }

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put(KMeansConstants.ARGS_DSIZE, Integer.toString(dsize));
    jobConfig.put(KMeansConstants.ARGS_CSIZE, Integer.toString(dsize));
    jobConfig.put(KMeansConstants.ARGS_WORKERS, Integer.toString(workers));
    jobConfig.put(KMeansConstants.ARGS_DINPUT_DIRECTORY, dFileName);
    jobConfig.put(KMeansConstants.ARGS_DINPUT_DIRECTORY, dFileName);
    jobConfig.put(KMeansConstants.ARGS_OUTPUT_DIRECTORY, outDir);
    jobConfig.put(KMeansConstants.ARGS_NUMBER_OF_FILES, numFiles);
    jobConfig.put(KMeansConstants.ARGS_SHARED_FILE_SYSTEM, shared);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("KMeans-job");
    jobBuilder.setWorkerClass(KMeansJob.class.getName());
    jobBuilder.addComputeResource(2, 512, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
