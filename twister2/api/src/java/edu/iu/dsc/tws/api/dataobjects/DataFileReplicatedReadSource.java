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
import edu.iu.dsc.tws.data.api.formatters.LocalCompleteTextInputPartitioner;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;

/**
 * This class is responsible for directly reading the data points from the respective filesystem
 * and add the datapoints into the DataObject.
 */
public class DataFileReplicatedReadSource extends BaseSource {

  private static final Logger LOG = Logger.getLogger(DataFileReplicatedReadSource.class.getName());

  private static final long serialVersionUID = -1L;

  /**
   * DataSource to partition the datapoints
   */
  private DataSource<?, ?> source;

  /**
   * Edge name to write the partitoned datapoints
   */
  private String edgeName;

  private String dataDirectory;

  public String getDataDirectory() {
    return dataDirectory;
  }

  public void setDataDirectory(String dataDirectory) {
    this.dataDirectory = dataDirectory;
  }

  public DataFileReplicatedReadSource(String edgename, String directory) {
    this.edgeName = edgename;
    this.dataDirectory = directory;
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


  public DataFileReplicatedReadSource() {
  }

  /**
   * The execute method uses the DataFileReader utils class in the data package to
   * read the input data points from the respective file system.
   */
  @Override
  public void execute() {
    LOG.fine("Context Task Index:" + context.taskIndex() + "\t" + getEdgeName());
    InputSplit<?> inputSplit = source.getNextSplit(context.taskIndex());
    while (inputSplit != null) {
      try {
        while (!inputSplit.reachedEnd()) {
          Object value = inputSplit.nextRecord(null);
          if (value != null) {
            context.write(getEdgeName(), value);
          }
        }
        inputSplit = source.getNextSplit(context.taskIndex());
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to read the input", e);
      }
    }
    context.end(getEdgeName());
  }

  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    ExecutionRuntime runtime = (ExecutionRuntime) cfg.get(ExecutorContext.TWISTER2_RUNTIME_OBJECT);
    this.source = runtime.createInput(cfg, context, new LocalCompleteTextInputPartitioner(
          new Path(getDataDirectory()), context.getParallelism(), config));
  }
}
