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
import edu.iu.dsc.tws.api.tset.fn.ArrowBasedSinkFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.data.arrow.Twister2ArrowFileWriter;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.SinkTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;

//import edu.iu.dsc.tws.api.tset.fn.ArrowBasedSinkFunc;

public class ArrowTSetSourceExample implements BatchTSetIWorker, Serializable {

  private static final Logger LOG = Logger.getLogger(ArrowTSetSourceExample.class.getName());

  private transient SourceTSet<Integer> pointSource;
  private transient Twister2ArrowFileWriter arrowWrite;

  private transient SinkTSet<Integer> pointSink;

  //private transient SourceTSet<String[]> csvSource;

  private transient Schema schema;

  @Override
  public void execute(BatchTSetEnvironment env) {
    Config config = env.getConfig();

    //TODO: FIX THE NULL POINTER EXCEPTION
    //int parallelism = (int) config.get("PARALLELISM");
    //String arrowInputFile = (String) config.get("ARROW_INPUT_FILE");
    int parallel = 2;
    String arrowInputFile = "/tmp/test.arrow";
    schema = makeSchema();
    LOG.info("schema value:" + schema);

    //TODO: WE HAVE TO MOVE THIS PART
//    LOG.info("parallelism and input file:" + parallel + "\t" + arrowInputFile);
//    try {
//      arrowWrite = new Twister2ArrowFileWriter(arrowInputFile, true);
//      arrowWrite.setUpTwister2ArrowWrite();
//    } catch (Exception e) {
//      throw new RuntimeException("Exception Occured", e);
//    }

    int dsize = 20;
    int dimension = 2;
    SourceTSet<String[]> csvSource = env.createCSVSource("/tmp/dinput", dsize, parallel, "split");

    /*ComputeTSet<Integer, Iterator<String[]>> datapoints = csvSource.direct().compute(
        new ComputeFunc<Integer, Iterator<String[]>>() {
          private int localPoints = 0;
          @Override
          public Integer compute(Iterator<String[]> input) {
            for (int i = 0; i < dsize / parallel && input.hasNext(); i++) {
              String[] value = input.next();
              for (int j = 0; j < value.length; j++) {
                localPoints = Integer.parseInt(value[j]);
              }
            }
            LOG.info("Integer value:" + localPoints);
            return localPoints;
          }
     });*/

    ComputeTSet<Integer, Iterator<String[]>> datapoints = csvSource.direct().compute(
        new ComputeFunc<Integer, Iterator<String[]>>() {
          private int localPoints = 0;

          @Override
          public Integer compute(Iterator<String[]> input) {
            for (int i = 0; i < dsize / parallel && input.hasNext(); i++) {
              String[] value = input.next();
              for (int j = 0; j < value.length; j++) {
                localPoints = Integer.parseInt(value[j]);
              }
            }
            LOG.info("Integer value:" + localPoints);
            return localPoints;
          }
        });

    //TODO: CHECK WITH NIRANDA
    pointSource = env.createArrowSource(arrowInputFile, parallel);

    SinkTSet<Integer> sinkTSet = pointSource.direct().sink(new ArrowBasedSinkFunc(
        arrowInputFile, parallel, schema) {
    });
    env.run(sinkTSet);
    ComputeTSet<List<Integer>, Iterator<Integer>> points = pointSource.direct().compute(
        new ComputeFunc<List<Integer>, Iterator<Integer>>() {
          private ArrayList<Integer> integers = new ArrayList<>();

          @Override
          public List<Integer> compute(Iterator<Integer> input) {
            input.forEachRemaining(integers::add);
            return integers;
          }
        });
    points.direct().forEach(s -> LOG.info("Double Array Values:" + s));
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
    options.addOption("input", "Arrow Input File");

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    String arrowInput = cmd.getOptionValue("input");
    int parallelism = Integer.parseInt(cmd.getOptionValue("parallelism", "2"));
    int workers = Integer.parseInt(cmd.getOptionValue("workers", "2"));

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();

    JobConfig jobConfig = new JobConfig();
    jobConfig.put("ARROW_INPUT_FILE", arrowInput);
    jobConfig.put("PARALLELISM", parallelism);
    jobConfig.put("WORKERS", workers);

    jobBuilder.setJobName("Arrow Testing Example");
    jobBuilder.setWorkerClass(ArrowTSetSourceExample.class);
    jobBuilder.addComputeResource(1, 512, 2, workers);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), ResourceAllocator.getDefaultConfig());
  }
}
