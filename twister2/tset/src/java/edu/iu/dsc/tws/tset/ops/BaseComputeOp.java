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
import edu.iu.dsc.tws.api.compute.nodes.ICompute;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.fn.TFunction;
import edu.iu.dsc.tws.tset.sets.BaseTSet;

/**
 * takes care of preparing the compute instances and creating the
 * {@link MultiEdgeOpAdapter}
 */
public abstract class BaseComputeOp<I> extends BaseOp implements ICompute<I>, Closable {
  private MultiEdgeOpAdapter multiEdgeOpAdapter;

  protected BaseComputeOp() {
  }

  protected BaseComputeOp(BaseTSet originTSet, Map<String, String> receivablesTSets) {
    super(originTSet, receivablesTSets);
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    gettSetContext().updateRuntimeInfo(cfg, ctx);
    this.getFunction().prepare(gettSetContext());

    this.multiEdgeOpAdapter = new MultiEdgeOpAdapter(ctx);
  }

  public abstract TFunction getFunction();

  protected <T> void writeToEdges(T output) {
    multiEdgeOpAdapter.writeToEdges(output);
  }

  protected void writeEndToEdges() {
    multiEdgeOpAdapter.writeEndToEdges();
  }

  <K, V> void keyedWriteToEdges(K key, V val) {
    multiEdgeOpAdapter.keyedWriteToEdges(key, val);
  }
}
