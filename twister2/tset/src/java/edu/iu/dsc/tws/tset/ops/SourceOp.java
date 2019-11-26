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

import java.util.Map;

import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.modifiers.Closable;
import edu.iu.dsc.tws.api.compute.nodes.ISource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.tset.sets.BaseTSet;

public class SourceOp<T> extends BaseOp implements ISource, Closable {
  protected MultiEdgeOpAdapter multiEdgeOpAdapter;
  protected SourceFunc<T> source;

  public SourceOp() {

  }

  public SourceOp(SourceFunc<T> src, BaseTSet originTSet, Map<String, String> receivables) {
    super(originTSet, receivables);
    this.source = src;
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    gettSetContext().updateRuntimeInfo(cfg, ctx);
    this.source.prepare(gettSetContext());
    this.multiEdgeOpAdapter = new MultiEdgeOpAdapter(ctx);
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
  public void close() {
    source.close();
  }
}
