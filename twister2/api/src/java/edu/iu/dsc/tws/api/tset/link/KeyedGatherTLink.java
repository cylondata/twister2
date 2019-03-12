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

package edu.iu.dsc.tws.api.tset.link;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.Constants;
import edu.iu.dsc.tws.api.tset.PartitionFunction;
import edu.iu.dsc.tws.api.tset.Selector;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.ops.TaskKeySelectorImpl;
import edu.iu.dsc.tws.api.tset.ops.TaskPartitionFunction;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;

public class KeyedGatherTLink<T, K> extends KeyValueTLink<T, K> {
  private BaseTSet<T> parent;

  private PartitionFunction<K> partitionFunction;

  private Selector<T, K> selector;

  public KeyedGatherTLink(Config cfg, TSetEnv tSetEnv, BaseTSet<T> prnt,
                          PartitionFunction<K> parFn, Selector<T, K> selc) {
    super(cfg, tSetEnv);
    this.parent = prnt;
    this.partitionFunction = parFn;
    this.selector = selc;
    this.name = "keyed-gather-" + parent.getName();
  }

  @Override
  public String getName() {
    return parent.getName();
  }

  public BaseTSet<T> getParent() {
    return parent;
  }

  @Override
  public boolean baseBuild() {
    return true;
  }

  public PartitionFunction<K> getPartitionFunction() {
    return partitionFunction;
  }

  public Selector<T, K> getSelector() {
    return selector;
  }

  public void buildConnection(ComputeConnection connection) {
    DataType keyType = TSetUtils.getDataType(getClassK());
    DataType dataType = TSetUtils.getDataType(getClassT());
    connection.keyedGather(parent.getName(), Constants.DEFAULT_EDGE, keyType, dataType,
        new TaskPartitionFunction<>(partitionFunction), new TaskKeySelectorImpl<>(selector));
  }

  @Override
  public KeyedGatherTLink<T, K> setName(String n) {
    super.setName(n);
    return this;
  }

  @Override
  public KeyedGatherTLink<T, K> setParallelism(int parallelism) {
    return null;
  }
}
