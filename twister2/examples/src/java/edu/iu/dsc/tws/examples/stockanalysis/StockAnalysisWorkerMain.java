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
package edu.iu.dsc.tws.examples.stockanalysis;

import java.util.HashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.stockanalysis.utils.StockAnalysisConstants;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;

public class StockAnalysisWorkerMain {

  public static void main(String[] args) throws ParseException {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 1);

    Options options = new Options();

    options.addOption(StockAnalysisConstants.WORKERS, true, "Workers");
    options.addOption(StockAnalysisConstants.DSIZE, true, "Size of the centroids file");
    options.addOption(StockAnalysisConstants.PARALLELISM_VALUE, true, "parallelism");
    options.addOption(StockAnalysisConstants.DIMENSIONS, true, "dimension of the matrix");
    options.addOption(StockAnalysisConstants.BYTE_TYPE, true, "bytetype");
    options.addOption(StockAnalysisConstants.DATA_INPUT, true, "datainput");

    options.addOption(StockAnalysisConstants.DINPUT_FILE, true, "input file");
    options.addOption(StockAnalysisConstants.DOUTPUT_DIRECTORY, true, "output directory");
    options.addOption(StockAnalysisConstants.NUMBER_OF_DAYS, true, "Number of days");
    options.addOption(StockAnalysisConstants.START_DATE, true, "start date");
    options.addOption(StockAnalysisConstants.END_DATE, true, "end date");
    options.addOption(StockAnalysisConstants.MODE, true, "mode");

    options.addOption(Utils.createOption(StockAnalysisConstants.DINPUT_DIRECTORY,
        true, "Matrix Input Creation directory", true));
    options.addOption(Utils.createOption(StockAnalysisConstants.FILE_SYSTEM,
        true, "file system", true));
    options.addOption(Utils.createOption(StockAnalysisConstants.CONFIG_FILE,
        true, "config File", true));

    @SuppressWarnings("deprecation")
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    int workers = Integer.parseInt(cmd.getOptionValue(StockAnalysisConstants.WORKERS));
    int dsize = Integer.parseInt(cmd.getOptionValue(StockAnalysisConstants.DSIZE));

    int dimension = Integer.parseInt(cmd.getOptionValue(StockAnalysisConstants.DIMENSIONS));
    int parallelismValue = Integer.parseInt(cmd.getOptionValue(
        StockAnalysisConstants.PARALLELISM_VALUE));

    String byteType = cmd.getOptionValue(StockAnalysisConstants.BYTE_TYPE);
    String dataDirectory = cmd.getOptionValue(StockAnalysisConstants.DINPUT_DIRECTORY);
    String fileSystem = cmd.getOptionValue(StockAnalysisConstants.FILE_SYSTEM);
    String configFile = cmd.getOptionValue(StockAnalysisConstants.CONFIG_FILE);
    String dataInput = cmd.getOptionValue(StockAnalysisConstants.DATA_INPUT);

    String inputFile = cmd.getOptionValue(StockAnalysisConstants.DINPUT_FILE);
    String outputDirectory = cmd.getOptionValue(StockAnalysisConstants.DOUTPUT_DIRECTORY);
    String numberOfDays = cmd.getOptionValue(StockAnalysisConstants.NUMBER_OF_DAYS);
    String mode = cmd.getOptionValue(StockAnalysisConstants.MODE);
    String startDate = cmd.getOptionValue(StockAnalysisConstants.START_DATE);
    String endDate = cmd.getOptionValue(StockAnalysisConstants.END_DATE);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put(StockAnalysisConstants.WORKERS, Integer.toString(workers));
    jobConfig.put(StockAnalysisConstants.PARALLELISM_VALUE, Integer.toString(parallelismValue));

    jobConfig.put(StockAnalysisConstants.DIMENSIONS, Integer.toString(dimension));
    jobConfig.put(StockAnalysisConstants.DSIZE, Integer.toString(dsize));

    jobConfig.put(StockAnalysisConstants.BYTE_TYPE, byteType);
    jobConfig.put(StockAnalysisConstants.DINPUT_DIRECTORY, dataDirectory);
    jobConfig.put(StockAnalysisConstants.FILE_SYSTEM, fileSystem);
    jobConfig.put(StockAnalysisConstants.CONFIG_FILE, configFile);
    jobConfig.put(StockAnalysisConstants.DATA_INPUT, dataInput);

    jobConfig.put(StockAnalysisConstants.DINPUT_FILE, inputFile);
    jobConfig.put(StockAnalysisConstants.DOUTPUT_DIRECTORY, outputDirectory);
    jobConfig.put(StockAnalysisConstants.MODE, mode);
    jobConfig.put(StockAnalysisConstants.START_DATE, startDate);
    jobConfig.put(StockAnalysisConstants.END_DATE, endDate);
    jobConfig.put(StockAnalysisConstants.NUMBER_OF_DAYS, numberOfDays);

    // build JobConfig
    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("StockAnalysisJob");
    jobBuilder.setWorkerClass(StockAnalysisWorker.class.getName());
    jobBuilder.addComputeResource(2, 2048, 1.0, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
