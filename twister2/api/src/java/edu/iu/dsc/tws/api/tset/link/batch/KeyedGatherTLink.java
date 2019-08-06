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

package edu.iu.dsc.tws.api.tset.link.batch;

import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.task.OperationNames;
import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;

public class KeyedGatherTLink<K, V> extends BIteratorLink<Tuple<K, Iterator<V>>> {
  private PartitionFunc<K> partitionFunction;

  public KeyedGatherTLink(BatchTSetEnvironment tSetEnv, int sourceParallelism) {
    this(tSetEnv, null, sourceParallelism);
  }

  public KeyedGatherTLink(BatchTSetEnvironment tSetEnv, PartitionFunc<K> partitionFn,
                          int sourceParallelism) {
    super(tSetEnv, TSetUtils.generateName("kgather"), sourceParallelism);
    this.partitionFunction = partitionFn;
  }

  @Override
  public Edge getEdge() {
    Edge e = new Edge(getId(), OperationNames.KEYED_GATHER, getMessageType());
    e.setKeyed(true);
    e.setPartitioner(partitionFunction);
    return e;
  }

  @Override
  public KeyedGatherTLink<K, V> setName(String n) {
    rename(n);
    return this;
  }
}
