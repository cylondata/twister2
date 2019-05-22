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
package edu.iu.dsc.tws.examples.batch.mds;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.InputPartitioner;
import edu.iu.dsc.tws.data.api.formatters.BinaryInputPartitioner;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;

public class MDSDataObjectSource extends BaseSource {

  private static final Logger LOG = Logger.getLogger(MDSDataObjectSource.class.getName());

  private static final long serialVersionUID = -1L;

  /**
   * DataSource to partition the datapoints
   */
  private DataSource<?, ?> source;

  private InputPartitioner inputPartitioner;

  /**
   * Edge name to write the partitoned datapoints
   */
  private String edgeName;
  private String dataDirectory;

  public int getDimension() {
    return dimension;
  }

  public void setDimension(int dimension) {
    this.dimension = dimension;
  }

  private int dimension;

  public MDSDataObjectSource(String edgename, String dataDirectory, int dim) {
    this.edgeName = edgename;
    this.dataDirectory = dataDirectory;
    this.dimension = dim;
  }

  public String getDataDirectory() {
    return dataDirectory;
  }

  public void setDataDirectory(String dataDirectory) {
    this.dataDirectory = dataDirectory;
  }

  /**
   * Getter property to set the edge name
   */
  public String getEdgeName() {
    return edgeName;
  }

  /**
   * Setter property to set the edge name
   */
  public void setEdgeName(String edgeName) {
    this.edgeName = edgeName;
  }

  /**
   * This method get the partitioned datapoints using the task index and write those values using
   * the respective edge name.
   */
  @Override
  public void execute() {

    double newSum = 0.0;
    int count = 0;

    Buffer buffer;
    byte[] line = new byte[2000];
    ByteBuffer byteBuffer = ByteBuffer.allocate(2000);
    byteBuffer.order(ByteOrder.BIG_ENDIAN);

//    InputSplit inputSplit = source.getNextSplit(context.taskIndex());
//    while (inputSplit != null) {
//      try {
//        while (!inputSplit.reachedEnd()) {
//          if (inputSplit.nextRecord(line) != null) {
//            LOG.fine("Array values are:" + Arrays.toString(line));
//            byteBuffer.clear();
//            byteBuffer.put(line);
//            byteBuffer.flip();
//            buffer = byteBuffer.asShortBuffer();
//            short[] shortArray = new short[1000];
//            ((ShortBuffer) buffer).get(shortArray);
//            for (short i : shortArray) {
//              newSum += i;
//              count++;
//            }
//          }
//        }
//        inputSplit = source.getNextSplit(context.taskIndex());
//      } catch (Exception ioe) {
//        throw new RuntimeException("IOException Occured:" + ioe.getMessage());
//      }
//      LOG.info("Task Index, sum and count values are:" + context.taskIndex()
//          + "\t" + newSum + "\t" + count);
//    }

    //Working Code
    try {
      InputSplit[] inputSplits = inputPartitioner.createInputSplits(context.getParallelism());
      InputSplitAssigner inputSplitAssigner = inputPartitioner.getInputSplitAssigner(inputSplits);
      InputSplit currentSplit;

      while ((currentSplit = inputSplitAssigner.getNextInputSplit("localhost",
          context.taskIndex())) != null) {
        currentSplit.open(this.config);
        while (currentSplit.nextRecord(line) != null) {
          byteBuffer.clear();
          byteBuffer.put(line);
          byteBuffer.flip();
          buffer = byteBuffer.asShortBuffer();
          short[] shortArray = new short[1000];
          ((ShortBuffer) buffer).get(shortArray);
          for (short i : shortArray) {
            newSum += i;
            count++;
          }
        }
      }
      LOG.info("Task Index, sum and count values are:" + context.taskIndex()
          + "\t" + newSum + "\t" + count);
    } catch (Exception e) {
      e.printStackTrace();
    }
    context.write(getEdgeName(), "hello finished splitting0");
    context.end(getEdgeName());
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    ExecutionRuntime runtime = (ExecutionRuntime) cfg.get(ExecutorContext.TWISTER2_RUNTIME_OBJECT);
    //this.source = runtime.createInput(cfg, context, new BinaryInputPartitioner(
    //    new Path(getDataDirectory()), 1000 * Short.BYTES, cfg));

    this.config = cfg;
    this.inputPartitioner = new BinaryInputPartitioner(new Path(getDataDirectory()),
        getDimension() * Short.BYTES);
    this.inputPartitioner.configure(cfg);
  }
}
