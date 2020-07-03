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

package edu.iu.dsc.tws.tset.sets.batch;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.tset.schema.KeyedSchema;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.ops.CheckpointedSourceOp;
import edu.iu.dsc.tws.tset.sources.DiskPartitionBackedSource;
import edu.iu.dsc.tws.tset.sources.DiskPartitionBackedSourceWrapper;

/**
 * This is a shadow {@link KeyedPersistedTSet} to add the checkpointing capability. It does not have
 * the sink that would store the data, because the purpose of this tset is to expose the data
 * that was stored by a {@link KeyedPersistedTSet}.
 * <p>
 * When this tset is executed, it would wrap {@link DiskPartitionBackedSource} from
 * {@link DiskPartitionBackedSourceWrapper} and return a {@link CheckpointedSourceOp} as the
 * {@link INode} for the underlying task.
 *
 * @param <K> tset key type
 */
public class KeyedCheckpointedTSet<K, V> extends KeyedPersistedTSet<K, V> {
  private DiskPartitionBackedSource<Tuple<K, V>> sourceFunc;

  public KeyedCheckpointedTSet(BatchEnvironment tSetEnv,
                               DiskPartitionBackedSource<Tuple<K, V>> sourceFn, int parallelism,
                               KeyedSchema inputSchema) {
    super(tSetEnv, null, parallelism, inputSchema);
    this.sourceFunc = sourceFn;
  }

  @Override
  public KeyedSourceTSet<K, V> getStoredSourceTSet() {
    if (storedSource == null) {
      storedSource = getTSetEnv().createKeyedSource(sourceFunc, getParallelism());
    }
    return storedSource;
  }

  /**
   * Reuses the {@link DiskPartitionBackedSourceWrapper} from non-keyed operations because there
   * will be no keyed writes to edges. This will only expose the
   * {@link edu.iu.dsc.tws.api.dataset.DataPartition} that will be called by the
   * {@link edu.iu.dsc.tws.task.impl.TaskExecutor}
   *
   * @return INode
   */
  @Override
  public INode getINode() {
    DiskPartitionBackedSourceWrapper<Tuple<K, V>> wrapper =
        new DiskPartitionBackedSourceWrapper<>(sourceFunc);
    return new CheckpointedSourceOp<>(wrapper, this, getInputs());
  }
}
