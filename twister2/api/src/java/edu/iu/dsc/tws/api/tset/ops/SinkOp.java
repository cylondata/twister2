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
package edu.iu.dsc.tws.api.tset.ops;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.modifiers.Closable;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.IComputableSink;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;

public class SinkOp<T> implements IComputableSink<T>, Closable, Collector, Receptor {
  private static final long serialVersionUID = -9398832570L;

  private SinkFunc<T> sink;

  private TSetContext tSetContext;

  public SinkOp(SinkFunc<T> sink) {
    this.sink = sink;
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    tSetContext = new TSetContext(cfg, ctx);
    sink.prepare(tSetContext);
  }

  @Override
  public boolean execute(IMessage<T> message) {
    sink.add(message.getContent());
    return true;
  }

  @Override
  public void close() {
    sink.close();
  }

  @Override
  public DataPartition<?> get() {
    return sink.get();
  }

  @Override
  public void add(String name, DataObject<?> data) {
    tSetContext.addInput(name, data);
  }
}
