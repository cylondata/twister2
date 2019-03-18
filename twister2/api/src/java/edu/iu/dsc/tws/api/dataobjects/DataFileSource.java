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
package edu.iu.dsc.tws.api.dataobjects;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.formatters.LocalTextInputPartitioner;
import edu.iu.dsc.tws.data.api.formatters.SharedTextInputPartitioner;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;

/**
 * This class is responsible for partition the datapoints which is based on the task parallelism
 * value. This class may use either the "LocalFixedInputPartitioner" or "LocalTextInputPartitioner"
 * to partition the datapoints. Finally, write the partitioned datapoints into their respective
 * edges.
 */
public class DataFileSource<T> extends BaseSource {

  private static final Logger LOG = Logger.getLogger(DataFileSource.class.getName());

  private static final long serialVersionUID = -1L;

  /**
   * DataSource to partition the datapoints
   */
  private DataSource<?, ?> source;

  /**
   * Edge name to write the partitoned datapoints
   */
  private String edgeName;

  public DataFileSource(String edgename) {
    this.edgeName = edgename;
  }

  /**
   * Getter property to set the edge name
   * @return
   */
  public String getEdgeName() {
    return edgeName;
  }

  /**
   * Setter property to set the edge name
   * @param edgeName
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
    LOG.fine("Context Task Index:" + context.taskIndex() + "\t" + getEdgeName());
    InputSplit<?> inputSplit = source.getNextSplit(context.taskIndex());
    int totalCount = 0;
    while (inputSplit != null) {
      try {
        int count = 0;
        while (!inputSplit.reachedEnd()) {
          Object value = inputSplit.nextRecord(null);
          if (value != null) {
            context.write(getEdgeName(), value);
            count += 1;
            totalCount += 1;
          }
        }
        inputSplit = source.getNextSplit(context.taskIndex());
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to read the input", e);
      }
    }
    context.end(getEdgeName());
  }

  /**
   * Retrieve the data input directory using the DataObjectConstants and the config file.
   * @param cfg
   * @param context
   */
  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    String datainputDirectory = cfg.getStringValue(DataObjectConstants.ARGS_CINPUT_DIRECTORY);
    ExecutionRuntime runtime = (ExecutionRuntime)
        cfg.get(ExecutorContext.TWISTER2_RUNTIME_OBJECT);
    boolean shared = cfg.getBooleanValue(DataObjectConstants.ARGS_SHARED_FILE_SYSTEM);
    int datasize = Integer.parseInt(cfg.getStringValue(DataObjectConstants.ARGS_CSIZE));
    if (!shared) {
      this.source = runtime.createInput(cfg, context,
          new LocalTextInputPartitioner(new Path(datainputDirectory),
              context.getParallelism(), config));
    } else {
      this.source = runtime.createInput(cfg, context,
          new SharedTextInputPartitioner(new Path(datainputDirectory),
              context.getParallelism(), config));
    }
  }
}
