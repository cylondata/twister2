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
package edu.iu.dsc.tws.examples.ml.svm.data;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.executor.ExecutorContext;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.api.formatters.LocalFixedInputPartitioner;
import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.dataset.DataSource;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;

public class SVMDataObjectSource<T, O extends InputSplit<T>> extends BaseSource {

  private static final Logger LOG = Logger.getLogger(SVMDataObjectSource.class.getName());

  private static final long serialVersionUID = -3569318984520808783L;

  private DataSource<T, O> source;

  /**
   * Edge name to write the partitoned datapoints
   */
  private String edgeName;
  private String dataDirectory;
  private int datasize;

  public SVMDataObjectSource(String edgeName, String dataDirectory) {
    this.edgeName = edgeName;
    this.dataDirectory = dataDirectory;
  }

  public SVMDataObjectSource(String edgeName, String dataDirectory, int dsize) {
    this.edgeName = edgeName;
    this.dataDirectory = dataDirectory;
    this.datasize = dsize;
  }

  public String getEdgeName() {
    return edgeName;
  }

  public void setEdgeName(String edgeName) {
    this.edgeName = edgeName;
  }

  public String getDataDirectory() {
    return dataDirectory;
  }

  public void setDataDirectory(String dataDirectory) {
    this.dataDirectory = dataDirectory;
  }

  @Override
  public void execute() {
    InputSplit<T> inputSplit = source.getNextSplit(context.taskIndex());
    while (inputSplit != null) {
      try {
        while (!inputSplit.reachedEnd()) {
          T t = inputSplit.nextRecord(null);
          if (t != null) {
            context.write(getEdgeName(), t);
          }
        }
        inputSplit = source.getNextSplit(context.taskIndex());
      } catch (IOException ex) {
        LOG.log(Level.SEVERE, "Failed to read the input", ex);
      }

    }
    context.end(getEdgeName());
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
    ExecutionRuntime runtime = (ExecutionRuntime) cfg.get(ExecutorContext.TWISTER2_RUNTIME_OBJECT);
    this.source = runtime.createInput(cfg, context, new LocalFixedInputPartitioner(
        new Path(getDataDirectory()), context.getParallelism(), config, this.datasize));
  }
}
