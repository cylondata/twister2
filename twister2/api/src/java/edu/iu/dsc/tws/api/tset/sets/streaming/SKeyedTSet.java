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

package edu.iu.dsc.tws.api.tset.sets.streaming;

import edu.iu.dsc.tws.api.task.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.link.streaming.SKeyedPartitionTLink;
import edu.iu.dsc.tws.api.tset.ops.BaseComputeOp;
import edu.iu.dsc.tws.api.tset.ops.MapToTupleIterOp;
import edu.iu.dsc.tws.api.tset.ops.MapToTupleOp;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;

/**
 * Attaches a key to the oncoming data.
 *
 * @param <K> key type
 * @param <V> data (value) type
 */
public class SKeyedTSet<K, V, T> extends BaseTSet<V> implements StreamingTupleTSet<K, V, T> {
  private BaseComputeOp<?> mapToTupleOp;

  public SKeyedTSet(TSetEnvironment tSetEnv,
                    MapToTupleIterOp<K, V, T> genTupleOp, int parallelism) {
    super(tSetEnv, TSetUtils.generateName("skeyed"), parallelism);
    this.mapToTupleOp = genTupleOp;
  }

  public SKeyedTSet(TSetEnvironment tSetEnv,
                    MapToTupleOp<K, V, T> genTupleOp, int parallelism) {
    super(tSetEnv, TSetUtils.generateName("skeyed"), parallelism);
    this.mapToTupleOp = genTupleOp;
  }

  @Override
  public SKeyedPartitionTLink<K, V> keyedPartition(PartitionFunc<K> partitionFn) {
    SKeyedPartitionTLink<K, V> partition = new SKeyedPartitionTLink<>(getTSetEnv(), partitionFn,
        getParallelism());
    addChildToGraph(partition);
    return partition;
  }

  @Override
  public SKeyedTSet<K, V, T> setName(String n) {
    rename(n);
    return this;
  }

  @Override
  public ICompute getINode() {
    return mapToTupleOp;
  }
}
