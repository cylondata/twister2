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
package edu.iu.dsc.tws.tset.sets.batch.row;

import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.tset.schema.RowSchema;
import edu.iu.dsc.tws.common.table.Row;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.ops.CheckpointedSourceOp;
import edu.iu.dsc.tws.tset.sources.DiskPartitionBackedSource;
import edu.iu.dsc.tws.tset.sources.DiskPartitionBackedSourceWrapper;

public class RowCheckpointedTSet extends RowPersistedTSet {
  private DiskPartitionBackedSource<Row> sourceFunc;

  public RowCheckpointedTSet(BatchTSetEnvironment tSetEnv,
                             DiskPartitionBackedSource<Row> sourceFn, int parallelism,
                             RowSchema inputSchema) {
    super(tSetEnv, null, parallelism, inputSchema);
    this.sourceFunc = sourceFn;
  }

  @Override
  public RowSourceTSet getStoredSourceTSet() {
    if (storedSource == null) {
      storedSource = getTSetEnv().createRowSource(getName(), sourceFunc,
          getParallelism());
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
    DiskPartitionBackedSourceWrapper<Row> wrapper =
        new DiskPartitionBackedSourceWrapper<>(sourceFunc);
    return new CheckpointedSourceOp<>(wrapper, this, getInputs());
  }
}
