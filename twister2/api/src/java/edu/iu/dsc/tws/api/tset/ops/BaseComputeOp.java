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

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.modifiers.Receptor;
import edu.iu.dsc.tws.api.task.nodes.BaseCompute;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.TFunction;

/**
 * takes care of preparing the compute instances and creating the out edges list
 */
public abstract class BaseComputeOp<I> extends BaseCompute<I> implements MultiOutEdgeOp, Receptor {

  private List<String> outEdges;

  private TSetContext tSetContext;

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
    this.outEdges = new ArrayList<>(ctx.getOutEdges().keySet());

    this.tSetContext = new TSetContext(cfg, ctx);
    this.getFunction().prepare(tSetContext);
  }

  public abstract TFunction getFunction();

  @Override
  public TaskContext getTaskContext() {
    return context;
  }

  @Override
  public List<String> getEdges() {
    return outEdges;
  }

  @Override
  public void add(String name, DataObject<?> data) {
    tSetContext.addInput(name, data);
  }
}
