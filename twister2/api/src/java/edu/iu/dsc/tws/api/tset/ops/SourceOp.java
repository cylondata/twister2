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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.modifiers.Receptor;
import edu.iu.dsc.tws.api.task.nodes.ISource;
import edu.iu.dsc.tws.api.tset.CacheableImpl;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.Source;

public class SourceOp<T> implements MultiOutEdgeOp, ISource, Receptor {
  private static final Logger LOG = Logger.getLogger(SourceOp.class.getName());

  private static final long serialVersionUID = -2400242961L;

  private TaskContext context;
  private List<String> outEdges;

  private Source<T> source;

  public SourceOp(Source<T> src) {
    this.source = src;
  }

  @Override
  public void execute() {
    if (source.hasNext()) {
      writeToEdges(source.next());
    } else {
      writeEndToEdges();
    }
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    this.context = ctx;
    this.outEdges = new ArrayList<>(ctx.getOutEdges().keySet());
    TSetContext tSetContext = new TSetContext(cfg, ctx);
    source.prepare(tSetContext);
  }

  @Override
  public void add(String name, DataObject<?> data) {
    source.addInput(name, new CacheableImpl<>(data));
  }

  @Override
  public TaskContext getContext() {
    return this.context;
  }

  @Override
  public List<String> getEdges() {
    return outEdges;
  }
}
