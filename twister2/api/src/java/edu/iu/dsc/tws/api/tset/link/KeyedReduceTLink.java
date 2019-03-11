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
package edu.iu.dsc.tws.api.tset.link;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.Constants;
import edu.iu.dsc.tws.api.tset.PartitionFunction;
import edu.iu.dsc.tws.api.tset.ReduceFunction;
import edu.iu.dsc.tws.api.tset.Selector;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.ops.ReduceOpFunction;
import edu.iu.dsc.tws.api.tset.ops.TaskKeySelectorImpl;
import edu.iu.dsc.tws.api.tset.ops.TaskPartitionFunction;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;

public class KeyedReduceTLink<T, K> extends KeyValueTLink<T, K> {
  private ReduceFunction<T> reduceFn;

  private BaseTSet<T> parent;

  private PartitionFunction<K> partitionFunction;

  private Selector<T, K> selector;

  public KeyedReduceTLink(Config cfg, TSetEnv tSetEnv, BaseTSet<T> prnt,
                          ReduceFunction<T> rFn, PartitionFunction<K> parFn,
                          Selector<T, K> selec) {
    super(cfg, tSetEnv);
    this.parent = prnt;
    this.reduceFn = rFn;
    this.partitionFunction = parFn;
    this.selector = selec;
    this.name = "keyed-reduce-" + parent.getName();
  }

  @Override
  public String getName() {
    return parent.getName();
  }

  @Override
  public boolean baseBuild() {
    return false;
  }

  public ReduceFunction<T> getReduceFn() {
    return reduceFn;
  }

  public PartitionFunction<K> getPartitionFunction() {
    return partitionFunction;
  }

  public void buildConnection(ComputeConnection connection) {
    DataType keyType = TSetUtils.getDataType(getClassK());
    DataType dataType = TSetUtils.getDataType(getClassT());
    connection.keyedReduce(parent.getName(), Constants.DEFAULT_EDGE,
        new ReduceOpFunction<>(reduceFn), keyType, dataType,
        new TaskPartitionFunction<K>(partitionFunction), new TaskKeySelectorImpl<>(selector));
  }
}
