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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;

public class MDSDataObjectSink  extends BaseSink implements Collector {

  private static final Logger LOG = Logger.getLogger(MDSDataObjectSink.class.getName());

  private short[] datavalues;

  @Override
  public boolean execute(IMessage content) {
    List<short[]> values = new ArrayList<>();
    while (((Iterator) content.getContent()).hasNext()) {
      values.add((short[]) ((Iterator) content.getContent()).next());
    }
    LOG.info("Distance Matrix (Row X Column) Length:" + values.size()
        + "\tX\t" + values.get(0).length);
    datavalues = new short[values.size()];
    for (short[] value : values) {
      datavalues = value;
    }
    return false;
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
  }

  @Override
  public DataPartition<short[]> get() {
    return new EntityPartition<>(context.taskIndex(), datavalues);
  }
}
