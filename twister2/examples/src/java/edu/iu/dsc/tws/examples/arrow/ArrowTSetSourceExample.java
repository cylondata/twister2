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
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.SinkTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;

public class ArrowTSetSourceExample implements BatchTSetIWorker, Serializable {

  private static final Logger LOG = Logger.getLogger(ArrowTSetSourceExample.class.getName());

  @Override
  public void execute(BatchTSetEnvironment env) {
    Config config = env.getConfig();
    String arrowInputFile = (String) config.get("ARROW_INPUT_FILE");
    String csvInputDirectory = (String) config.get("CSV_INPUT_DIRECTORY");
    int parallel = (int) config.get("PARALLELISM");
    int dsize = (int) config.get("DSIZE");
    LOG.info("parallelism and input file:" + parallel + "\t" + arrowInputFile + "\t"
        + csvInputDirectory);

    Schema schema = makeSchema();
    SourceTSet<String[]> csvSource = env.createCSVSource(
        csvInputDirectory, dsize, parallel, "split");
    SinkTSet<Iterator<Integer>> sinkTSet = csvSource
        .direct()
        .flatmap(
            (FlatMapFunc<Integer, String[]>) (input, collector) -> {
              for (String s : input) {
                collector.collect(Integer.parseInt(s.trim()));
              }
            })
        .direct()
        .sink(new ArrowBasedSinkFunc<>(arrowInputFile, schema.toJson()));

    // run sink explicitly
    env.run(sinkTSet);

    env.createArrowSource(arrowInputFile, parallel, schema.toJson())
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
    LOG.log(Level.INFO, "Starting CSV Source Job");

    Options options = new Options();
    options.addOption("parallelism", true, "Parallelism");
    options.addOption("workers", true, "Workers");
    options.addOption("dsize", true, "100");
    options.addOption("input", "Arrow Input File");
    options.addOption("csvdirectory", "CSV Input Directory");

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    String arrowInput = cmd.getOptionValue("input", "tmp.arrow");
    String csvInputDirectory = cmd.getOptionValue("csvdirectory", "/tmp/dinput");
    int parallelism = Integer.parseInt(cmd.getOptionValue("parallelism", "2"));
    int workers = Integer.parseInt(cmd.getOptionValue("workers", "2"));
    int dsize = Integer.parseInt(cmd.getOptionValue("dsize", "100"));

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();

    JobConfig jobConfig = new JobConfig();
    jobConfig.put("ARROW_INPUT_FILE", arrowInput);
    jobConfig.put("PARALLELISM", parallelism);
    jobConfig.put("WORKERS", workers);
    jobConfig.put("DSIZE", dsize);
    jobConfig.put("CSV_INPUT_DIRECTORY", csvInputDirectory);

    jobBuilder.setJobName("Arrow Testing Example");
    jobBuilder.setWorkerClass(ArrowTSetSourceExample.class);
    jobBuilder.addComputeResource(1, 512, 2, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), ResourceAllocator.getDefaultConfig());
  }
}
