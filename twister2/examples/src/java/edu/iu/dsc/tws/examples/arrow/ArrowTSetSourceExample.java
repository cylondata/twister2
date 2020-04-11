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
package edu.iu.dsc.tws.examples.arrow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.ImmutableList;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.impl.ArrowBasedSinkFunc;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.SinkTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;

//import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;

public class ArrowTSetSourceExample implements BatchTSetIWorker, Serializable {

  private static final Logger LOG = Logger.getLogger(ArrowTSetSourceExample.class.getName());

  @Override
  public void execute(BatchTSetEnvironment env) {
    Config config = env.getConfig();
    String csvInputDirectory = config.getStringValue(DataObjectConstants.DINPUT_DIRECTORY);
    String arrowInputDirectory = config.getStringValue(DataObjectConstants.ARROW_DIRECTORY);
    String arrowFileName = config.getStringValue(DataObjectConstants.FILE_NAME);
    int workers = config.getIntegerValue(DataObjectConstants.WORKERS);
    int parallel = config.getIntegerValue(DataObjectConstants.PARALLELISM_VALUE);
    int dsize = config.getIntegerValue(DataObjectConstants.DSIZE);

    LOG.info("arrow input file:" + arrowFileName + "\t" + arrowInputDirectory + "\t"
        + csvInputDirectory + "\t" + workers + "\t" + parallel);

    Schema schema = makeSchema();
    //TODO: We have to use the single dimensional value check with Niranda
    SourceTSet<String[]> csvSource
        = env.createCSVSource(csvInputDirectory, dsize, parallel, "split");
    SinkTSet<Iterator<Integer>> sinkTSet = csvSource
        .direct()
        .flatmap(
            (FlatMapFunc<Integer, String[]>) (input, collector) -> {
              for (String s : input) {
                collector.collect(Integer.parseInt(s.trim()));
              }
            })
        .direct()
        .sink(new ArrowBasedSinkFunc<>(arrowInputDirectory, arrowFileName, schema.toJson()));

    // run sink explicitly
    env.run(sinkTSet);

    env.createArrowSource(arrowInputDirectory, arrowFileName, parallel, schema.toJson())
        .direct()
        .compute(
            new ComputeFunc<List<Object>, Iterator<Object>>() {
              private final ArrayList<Object> integers = new ArrayList<>();
              @Override
              public List<Object> compute(Iterator<Object> input) {
                input.forEachRemaining(integers::add);
                return integers;
              }
            })
        .direct()
        .forEach(s -> LOG.info("Integer Array Values:" + s));
  }

  private Schema makeSchema() {
    ImmutableList.Builder<Field> builder = ImmutableList.builder();
    builder.add(new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null));
    return new Schema(builder.build(), null);
  }

  public static void main(String[] args) throws Exception {
    LOG.log(Level.INFO, "Starting Twister2 Arrow Job");

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    Options options = new Options();
    options.addOption(Utils.createOption(DataObjectConstants.PARALLELISM_VALUE, true,
        "Parallelism", true));
    options.addOption(Utils.createOption(DataObjectConstants.WORKERS, true,
        "Workers", true));
    options.addOption(Utils.createOption(DataObjectConstants.DSIZE, true,
        "100", true));
    options.addOption(Utils.createOption(DataObjectConstants.DINPUT_DIRECTORY, true,
        "CSV Input Directory", true));
    options.addOption(Utils.createOption(DataObjectConstants.ARROW_DIRECTORY, true,
        "Arrow Input Directory", true));
    options.addOption(Utils.createOption(DataObjectConstants.FILE_NAME, true,
        "Arrow File Name", true));

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    int parallelism = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.PARALLELISM_VALUE));
    int workers = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.WORKERS));
    int dsize = Integer.parseInt(cmd.getOptionValue(DataObjectConstants.DSIZE));
    String csvInputDirectory = cmd.getOptionValue(DataObjectConstants.DINPUT_DIRECTORY);
    String arrowInputDirectory = cmd.getOptionValue(DataObjectConstants.ARROW_DIRECTORY);
    String arrowFileName = cmd.getOptionValue(DataObjectConstants.FILE_NAME);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();

    JobConfig jobConfig = new JobConfig();
    jobConfig.put(DataObjectConstants.PARALLELISM_VALUE, parallelism);
    jobConfig.put(DataObjectConstants.WORKERS, workers);
    jobConfig.put(DataObjectConstants.DSIZE, dsize);
    jobConfig.put(DataObjectConstants.DINPUT_DIRECTORY, csvInputDirectory);
    jobConfig.put(DataObjectConstants.ARROW_DIRECTORY, arrowInputDirectory);
    jobConfig.put(DataObjectConstants.FILE_NAME, arrowFileName);

    jobBuilder.setJobName("Arrow Testing Example");
    jobBuilder.setWorkerClass(ArrowTSetSourceExample.class);
    jobBuilder.addComputeResource(1, 512, 2, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
