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

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.out.TextOutputWriter;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.DataSink;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;

/**
 * This class receives the message from the DataFileSource and writes the output into the DataObject
 */
public class DataFileSink<T> extends BaseSink implements Collector {

  private static final Logger LOG = Logger.getLogger(DataFileSink.class.getName());

  private static final long serialVersionUID = -1L;

  private DataSink<String> datasink = null;

  /**
   * This method add the received message from the Data File Object into the data objects.
   * @param message
   * @return
   */
  @Override
  public boolean execute(IMessage message) {
    datasink.add(context.taskIndex(), String.valueOf(message.getContent()));
    datasink.persist();
    return true;
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    String outDir = cfg.getStringValue(DataObjectConstants.ARGS_OUTPUT_DIRECTORY);
    this.datasink = new DataSink<>(cfg,
        new TextOutputWriter(FileSystem.WriteMode.OVERWRITE, new Path(outDir)));
  }

  @Override
  public DataPartition<Object> get() {
    return new EntityPartition<>(context.taskIndex(), datasink);
  }
}
