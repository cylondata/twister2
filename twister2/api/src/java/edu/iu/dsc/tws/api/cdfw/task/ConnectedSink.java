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
package edu.iu.dsc.tws.api.cdfw.task;

import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.dataset.PSet;
import edu.iu.dsc.tws.dataset.impl.CollectionPSet;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;

public class ConnectedSink extends BaseSink implements Collector<Object> {
  /**
   * The name of the data set
   */
  private String outName;

  /**
   * The partition to use
   */
  private CollectionPSet<Object> partition;

  public ConnectedSink() {
  }

  public ConnectedSink(String outName) {
    this.outName = outName;
  }

  @Override
  public PSet<Object> get() {
    return partition;
  }

  @Override
  public PSet<Object> get(String name) {
    if (name.equals(outName)) {
      return partition;
    } else {
      throw new RuntimeException("Un-expected name: " + name);
    }
  }

  @Override
  public boolean execute(IMessage message) {
    partition.add(message.getContent());
    return true;
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
    partition = new CollectionPSet<>(ctx.getWorkerId(), ctx.taskIndex());
  }

  public String getOutName() {
    return outName;
  }

  public void setOutName(String outName) {
    this.outName = outName;
  }
}
