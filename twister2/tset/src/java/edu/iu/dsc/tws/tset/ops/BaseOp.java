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

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import edu.iu.dsc.tws.api.compute.modifiers.IONames;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.tset.sets.BaseTSet;

/**
 * Every tset op is a Receptor. This class handles that. This also takes care of creating/updating
 * {@link edu.iu.dsc.tws.api.tset.TSetContext}.
 */
public abstract class BaseOp implements Receptor, Serializable {
  private TSetContext tSetContext = new TSetContext();
  // keys of the data partitions this op receives
  private IONames receivables;

  // map (TSetID --> input Name)
  private Map<String, String> rcvTSets;

  BaseOp() {
  }

  BaseOp(BaseTSet originTSet) {
    this(originTSet, Collections.emptyMap());
  }

  BaseOp(BaseTSet originTSet, Map<String, String> receivableTSets) {
    this.receivables = IONames.declare(receivableTSets.keySet());
    this.rcvTSets = receivableTSets;

    if (originTSet != null) {
      this.tSetContext.settSetId(originTSet.getId());
      this.tSetContext.settSetName(originTSet.getName());
      this.tSetContext.setParallelism(originTSet.getParallelism());
    }
  }

  @Override
  public void add(String key, DataPartition<?> data) {
    // when it is sent to the tset context, users would not know about the key here. Therefore,
    // translate it to the user specified key
    this.tSetContext.addInput(rcvTSets.get(key), data);
  }

  @Override
  public IONames getReceivableNames() {
    return receivables;
  }

  TSetContext gettSetContext() {
    return tSetContext;
  }
}
