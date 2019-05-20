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

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.formatters.BinaryInputPartitioner;
import edu.iu.dsc.tws.data.api.formatters.LocalBinaryInputPartitioner;
import edu.iu.dsc.tws.data.api.splits.FileInputSplit;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;

public class MDSDataObjectSource extends BaseSource {

  private static final Logger LOG = Logger.getLogger(DataObjectSource.class.getName());

  private static final long serialVersionUID = -1L;

  /**
   * DataSource to partition the datapoints
   */
  private DataSource<?, ?> source;

  private BinaryInputPartitioner inputPartitioner;
  private FileInputSplit[] splits;
  //private InputSplit currentSplit;

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

    int minSplits = 8;
    double expectedSum = 1.97973979E8;
    double newSum = 0.0;
    int count = 0;
    Buffer buffer = null;

    InputSplit currentSplit = source.getNextSplit(context.taskIndex());
    byte[] line = new byte[4000];
    ByteBuffer byteBuffer = ByteBuffer.allocate(4000);
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    while (currentSplit != null) {
      try {
        while (!currentSplit.reachedEnd()) {
          if (currentSplit.nextRecord(line) != null) {
            byteBuffer.clear();
            byteBuffer.put(line);
            byteBuffer.flip();
            buffer = byteBuffer.asShortBuffer();
            short[] shortArray = new short[2000];
            ((ShortBuffer) buffer).get(shortArray);
            for (short i : shortArray) {
              newSum += i;
              count++;
            }
          }
        }//End of while
        currentSplit = source.getNextSplit(context.taskIndex());
      } catch (IOException ioe) {
        throw new RuntimeException("IOException Occured:" + ioe.getMessage());
      }
      LOG.info("%d New Sum:" + newSum);
      LOG.info("%d Count:" + count);
    }
    context.end(getEdgeName());
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    ExecutionRuntime runtime = (ExecutionRuntime) cfg.get(ExecutorContext.TWISTER2_RUNTIME_OBJECT);
    this.source = runtime.createInput(cfg, context, new LocalBinaryInputPartitioner(
        new Path(getDataDirectory()), context.getParallelism(), 2000 * Short.BYTES, cfg));
  }
}
