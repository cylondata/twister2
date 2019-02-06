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
    options.addOption(KMeansConstants.ARGS_DIMENSIONS, true, "dim");
    options.addOption(KMeansConstants.ARGS_PARALLELISM_VALUE, true, "parallelism");
    options.addOption(KMeansConstants.ARGS_NUMBER_OF_CLUSTERS, true, "clusters");

    options.addOption(Utils.createOption(KMeansConstants.ARGS_DINPUT_DIRECTORY,
        true, "Data points Input directory", true));
    options.addOption(Utils.createOption(KMeansConstants.ARGS_CINPUT_DIRECTORY,
        true, "Centroids Input directory", true));
    options.addOption(Utils.createOption(KMeansConstants.ARGS_OUTPUT_DIRECTORY,
        true, "Output directory", true));
    options.addOption(Utils.createOption(KMeansConstants.ARGS_FILE_SYSTEM,
        true, "file system", true));

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    int workers = Integer.parseInt(cmd.getOptionValue(KMeansConstants.ARGS_WORKERS));
    int dsize = Integer.parseInt(cmd.getOptionValue(KMeansConstants.ARGS_DSIZE));
    int csize = Integer.parseInt(cmd.getOptionValue(KMeansConstants.ARGS_CSIZE));
    int numFiles = Integer.parseInt(cmd.getOptionValue(KMeansConstants.ARGS_NUMBER_OF_FILES));
    int dimension = Integer.parseInt(cmd.getOptionValue(KMeansConstants.ARGS_DIMENSIONS));
    int parallelismValue = Integer.parseInt(cmd.getOptionValue(
        KMeansConstants.ARGS_PARALLELISM_VALUE));
    int numberOfClusters = Integer.parseInt(cmd.getOptionValue(
        KMeansConstants.ARGS_NUMBER_OF_CLUSTERS));

    String dataDirectory = cmd.getOptionValue(KMeansConstants.ARGS_DINPUT_DIRECTORY);
    String centroidDirectory = cmd.getOptionValue(KMeansConstants.ARGS_CINPUT_DIRECTORY);
    String outputDirectory = cmd.getOptionValue(KMeansConstants.ARGS_OUTPUT_DIRECTORY);
    String fileSystem = cmd.getOptionValue(KMeansConstants.ARGS_FILE_SYSTEM);

    boolean shared =
        Boolean.parseBoolean(cmd.getOptionValue(KMeansConstants.ARGS_SHARED_FILE_SYSTEM));
    
    // we we are a shared file system, lets generate data at the client
    /*if (shared) {
      KMeansDataGenerator.generateData(
          "txt", new Path(dataDirectory), numFiles, dsize, 100, dimension);
      KMeansDataGenerator.generateData(
          "txt", new Path(centroidDirectory), numFiles, csize, 100, dimension);
    }*/

    // build JobConfig
    JobConfig jobConfig = new JobConfig();

    jobConfig.put(KMeansConstants.ARGS_DINPUT_DIRECTORY, dataDirectory);
    jobConfig.put(KMeansConstants.ARGS_CINPUT_DIRECTORY, centroidDirectory);
    jobConfig.put(KMeansConstants.ARGS_OUTPUT_DIRECTORY, outputDirectory);
    jobConfig.put(KMeansConstants.ARGS_FILE_SYSTEM, fileSystem);
    jobConfig.put(KMeansConstants.ARGS_DSIZE, Integer.toString(dsize));
    jobConfig.put(KMeansConstants.ARGS_CSIZE, Integer.toString(csize));
    jobConfig.put(KMeansConstants.ARGS_WORKERS, Integer.toString(workers));
    jobConfig.put(KMeansConstants.ARGS_NUMBER_OF_FILES, Integer.toString(numFiles));
    jobConfig.put(KMeansConstants.ARGS_DIMENSIONS, Integer.toString(dimension));
    jobConfig.put(KMeansConstants.ARGS_PARALLELISM_VALUE, Integer.toString(parallelismValue));
    jobConfig.put(KMeansConstants.ARGS_SHARED_FILE_SYSTEM, shared);
    jobConfig.put(KMeansConstants.ARGS_NUMBER_OF_CLUSTERS, Integer.toString(numberOfClusters));

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("KMeans-job");
    jobBuilder.setWorkerClass(KMeansDataParallelWorker.class.getName());
    //jobBuilder.setWorkerClass(KMeansCentroidParallelWorker.class.getName());
    jobBuilder.addComputeResource(2, 512, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
