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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.ImmutableList;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.Types;
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
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.data.arrow.Twister2ArrowWrite;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;

public class ArrowTSetSourceExample implements BatchTSetIWorker, Serializable {

  private static final Logger LOG = Logger.getLogger(ArrowTSetSourceExample.class.getName());

  private SourceTSet<Integer> pointSource;

  private Twister2ArrowWrite arrowWrite;
  private RootAllocator rootAllocator;

  @Override
  public void execute(BatchTSetEnvironment env) {
    String arrowInputFile = "/home/kannan/test.arrow";
    try {
      arrowWrite = new Twister2ArrowWrite(arrowInputFile, true);
      arrowWrite.setUpTwister2ArrowWrite();
      Thread.sleep(1000);
    } catch (Exception e) {
      throw new RuntimeException("Exception Occured", e);
    }
////////////////////////////////////////////////////////////
//    int parallelism = 1;
//    schema = makeSchema();
//    pointSource = env.createArrowSource(
//        "/home/kannan/test.arrow", parallelism, schema);
//    pointSource.direct().cache();
//    ComputeTSet<Integer[], Iterator<Integer>> points = pointSource.direct().compute(
//        new ComputeFunc<Integer[], Iterator<Integer>>() {
//          private Integer[] integers = new Integer[100];
//          @Override
//          public Integer[] compute(Iterator<Integer> input) {
//            for (int i = 0; i < 100 && input.hasNext(); i++) {
//              Integer value = input.next();
//              integers[i] = value;
//            }
//            LOG.info("Double Array Values:" + Arrays.deepToString(integers));
//            return integers;
//          }
//        });
//    points.direct().forEach(s -> { });

    /////////////////////////Testing to read the file
    rootAllocator = new RootAllocator(Integer.MAX_VALUE);
    FileInputStream fileInputStream = null;
    ArrowFileReader arrowFileReader = null;
    try {
      fileInputStream = new FileInputStream(arrowInputFile);
      arrowFileReader = new ArrowFileReader(new SeekableReadChannel(
          fileInputStream.getChannel()), rootAllocator);
      System.out.println("\nReading the arrow file : " + arrowInputFile);
      VectorSchemaRoot root = arrowFileReader.getVectorSchemaRoot();
      LOG.info("File size : " + arrowInputFile.length()
          + " schema is " + root.getSchema().toString());
      List<ArrowBlock> arrowBlocks = arrowFileReader.getRecordBlocks();
      LOG.info("Number of arrow blocks are " + arrowBlocks.size());
      for (int i = 0; i < arrowBlocks.size(); i++) {
        LOG.info("\t[" + i + "] row count for this block is " + root.getRowCount());
        List<FieldVector> fieldVector = root.getFieldVectors();
        LOG.info("\t[" + i + "] number of fieldVectors (corresponding to columns) : "
            + fieldVector.size());
        for (int j = 0; j < fieldVector.size(); j++) {
          Types.MinorType mt = fieldVector.get(j).getMinorType();
          switch (mt) {
            case INT:
              showIntAccessor(fieldVector.get(j));
              break;
            default:
              throw new Exception(" MinorType " + mt);
          }
        }
      }
      arrowFileReader.close();
    } catch (FileNotFoundException e) {
      throw new Twister2RuntimeException("File Not Found", e);
    } catch (Exception ioe) {
      throw new Twister2RuntimeException("IOException Occured", ioe);
    }
  }

  private void showIntAccessor(FieldVector fx) {
    IntVector intVector = (IntVector) fx;
    for (int j = 0; j < intVector.getValueCount(); j++) {
      if (!intVector.isNull(j)) {
        int value = intVector.get(j);
        LOG.info("\t\t intAccessor[" + j + "] " + value);
      } else {
        LOG.info("\t\t intAccessor[" + j + "] : NULL ");
      }
    }
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

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);
    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();

    JobConfig jobConfig = new JobConfig();
    jobBuilder.setJobName("arrowtest");
    jobBuilder.setWorkerClass(ArrowTSetSourceExample.class);
    jobBuilder.addComputeResource(1, 512, 2);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), ResourceAllocator.getDefaultConfig());
  }
}
