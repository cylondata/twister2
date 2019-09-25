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
package edu.iu.dsc.tws.tset.ops;

import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.ISource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;

public class SourceOp<T> implements ISource, Receptor {
  private static final long serialVersionUID = -2400242961L;

  private TaskContext context;
  private TSetContext tSetContext;
  private MultiEdgeOpAdapter multiEdgeOpAdapter;

  private SourceFunc<T> source;

  public SourceOp() {
  }
  public SourceOp(SourceFunc<T> src) {
    this.source = src;
  }

  @Override
  public void execute() {
    if (source.hasNext()) {
      multiEdgeOpAdapter.writeToEdges(source.next());
    } else {
      multiEdgeOpAdapter.writeEndToEdges();
    }
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    this.context = ctx;
    this.multiEdgeOpAdapter = new MultiEdgeOpAdapter(ctx);
    this.tSetContext = new TSetContext(cfg, ctx);

    this.source.prepare(tSetContext);
  }

  @Override
  public void add(String name, DataObject<?> data) {
    this.tSetContext.addInput(name, data);
  }
}
