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
package edu.iu.dsc.tws.examples.batch.kmeans;

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
import edu.iu.dsc.tws.api.dataobjects.DataObjectConstants;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

public class KMeansWorkerMain {

  private static final Logger LOG = Logger.getLogger(KMeansWorkerMain.class.getName());

  public static void main(String[] args) throws ParseException {
    LOG.log(Level.INFO, "KMeans Clustering Job");

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    Options options = new Options();
    options.addOption(DataObjectConstants.ARGS_WORKERS, true, "Workers");
    options.addOption(DataObjectConstants.ARGS_CSIZE, true, "Size of the dapoints file");
    options.addOption(DataObjectConstants.ARGS_DSIZE, true, "Size of the centroids file");
    options.addOption(DataObjectConstants.ARGS_NUMBER_OF_FILES, true, "Number of files");
    options.addOption(DataObjectConstants.ARGS_SHARED_FILE_SYSTEM, false, "Shared file system");
    options.addOption(DataObjectConstants.ARGS_DIMENSIONS, true, "dim");
    options.addOption(DataObjectConstants.ARGS_PARALLELISM_VALUE, true, "parallelism");
    options.addOption(DataObjectConstants.ARGS_ITERATIONS, true, "iter");

    options.addOption(Utils.createOption(DataObjectConstants.ARGS_DINPUT_DIRECTORY,
        true, "Data points Input directory", true));
    options.addOption(Utils.createOption(DataObjectConstants.ARGS_CINPUT_DIRECTORY,
        true, "Centroids Input directory", true));
    options.addOption(Utils.createOption(DataObjectConstants.ARGS_OUTPUT_DIRECTORY,
        true, "Output directory", true));
    options.addOption(Utils.createOption(DataObjectConstants.ARGS_FILE_SYSTEM,
        true, "file system", true));

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    int workers = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.ARGS_WORKERS));
    int dsize = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.ARGS_DSIZE));
    int csize = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.ARGS_CSIZE));
    int numFiles = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.ARGS_NUMBER_OF_FILES));
    int dimension = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.ARGS_DIMENSIONS));
    int parallelismValue = Integer.parseInt(cmd.getOptionValue(
        DataObjectConstants.ARGS_PARALLELISM_VALUE));
    int iterations = Integer.parseInt(cmd.getOptionValue(
        DataObjectConstants.ARGS_ITERATIONS));

    String dataDirectory = cmd.getOptionValue(DataObjectConstants.ARGS_DINPUT_DIRECTORY);
    String centroidDirectory = cmd.getOptionValue(DataObjectConstants.ARGS_CINPUT_DIRECTORY);
    String outputDirectory = cmd.getOptionValue(DataObjectConstants.ARGS_OUTPUT_DIRECTORY);
    String fileSystem = cmd.getOptionValue(DataObjectConstants.ARGS_FILE_SYSTEM);

    boolean shared =
        Boolean.parseBoolean(cmd.getOptionValue(DataObjectConstants.ARGS_SHARED_FILE_SYSTEM));

    // build JobConfig
    JobConfig jobConfig = new JobConfig();

    jobConfig.put(DataObjectConstants.ARGS_DINPUT_DIRECTORY, dataDirectory);
    jobConfig.put(DataObjectConstants.ARGS_CINPUT_DIRECTORY, centroidDirectory);
    jobConfig.put(DataObjectConstants.ARGS_OUTPUT_DIRECTORY, outputDirectory);
    jobConfig.put(DataObjectConstants.ARGS_FILE_SYSTEM, fileSystem);
    jobConfig.put(DataObjectConstants.ARGS_DSIZE, Integer.toString(dsize));
    jobConfig.put(DataObjectConstants.ARGS_CSIZE, Integer.toString(csize));
    jobConfig.put(DataObjectConstants.ARGS_WORKERS, Integer.toString(workers));
    jobConfig.put(DataObjectConstants.ARGS_NUMBER_OF_FILES, Integer.toString(numFiles));
    jobConfig.put(DataObjectConstants.ARGS_DIMENSIONS, Integer.toString(dimension));
    jobConfig.put(DataObjectConstants.ARGS_PARALLELISM_VALUE, Integer.toString(parallelismValue));
    jobConfig.put(DataObjectConstants.ARGS_SHARED_FILE_SYSTEM, shared);
    jobConfig.put(DataObjectConstants.ARGS_ITERATIONS, Integer.toString(iterations));

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("KMeans-job");
    jobBuilder.setWorkerClass(KMeansWorker.class.getName());
    jobBuilder.addComputeResource(2, 512, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
