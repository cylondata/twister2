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
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;

public class DataObjectSink<T> extends BaseSink implements Collector {

  private static final Logger LOG = Logger.getLogger(DataObjectSink.class.getName());

  private static final long serialVersionUID = -1L;

  private DataObject<Object> datapoints = null;

  @Override
  public boolean execute(IMessage message) {
    datapoints.addPartition(new EntityPartition<>(context.taskIndex(), message.getContent()));
    return true;
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    this.datapoints = new DataObjectImpl<>(config);
  }

  @Override
  public DataPartition<Object> get() {
    return new EntityPartition<>(context.taskIndex(), datapoints);
  }
}
