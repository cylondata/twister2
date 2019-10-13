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

import java.util.Collections;
import java.util.Set;

import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.modifiers.IONames;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.tset.sets.BaseTSet;

/**
 * Every tset op is a Receptor. This class handles that. This also takes care of creating/updating
 * {@link edu.iu.dsc.tws.api.tset.TSetContext}.
 */
public abstract class BaseOp implements Receptor {
  private TSetContext tSetContext = new TSetContext();
  // keys of the data partitions this op receives
  private IONames receivables;

  BaseOp() {
  }

  BaseOp(BaseTSet originTSet) {
    this(originTSet, Collections.emptySet());
  }

  BaseOp(BaseTSet originTSet, Set<String> receivablesNames) {
    this.receivables = IONames.declare(receivablesNames);

    this.tSetContext.settSetId(originTSet.getId());
    this.tSetContext.settSetName(originTSet.getName());
    this.tSetContext.setParallelism(originTSet.getParallelism());
  }

  @Override
  public void add(String name, DataPartition<?> data) {
    this.tSetContext.addInput(name, data);
  }

  @Override
  public IONames getReceivableNames() {
    return receivables;
  }

  void updateTSetContext(Config cfg, TaskContext taskCtx) {
    this.tSetContext.setConfig(cfg);
    this.tSetContext.settSetIndex(taskCtx.taskIndex());
  }

  TSetContext gettSetContext() {
    return tSetContext;
  }
}
