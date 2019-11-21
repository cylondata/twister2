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

import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.dataset.partition.DiskBackedCollectionPartition;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sources.DiskPartitionBackedSource;

/**
 * This is a shadow {@link PersistedTSet} to add the checkpointing capability. It does not have
 * the sink that would store the data, because the purpose of this tset is to expose the data
 * that was stored by a {@link PersistedTSet}.
 *
 * todo check how this works with inputs! we may have to pass a dummy sink that exposes the data
 *  partition that has the data
 * @param <T> tset type
 */
public class CheckpointedTSet<T> extends PersistedTSet<T> {
  private DiskPartitionBackedSource<T> sourceFunc;

  public CheckpointedTSet(BatchTSetEnvironment tSetEnv, String name,
                          int parallelism, SourceTSet<T> source) {
    super(tSetEnv, null, parallelism);
    super.storedSource = source;
  }

  public CheckpointedTSet(BatchTSetEnvironment tSetEnv, DiskPartitionBackedSource<T> sourceFn,
                          int parallelism) {
    super(tSetEnv, null, parallelism);
    this.sourceFunc = sourceFn;
  }

  @Override
  public SourceTSet<T> getStoredSourceTSet() {
    if (storedSource == null) {
      storedSource = getTSetEnv().createSource(sourceFunc, getParallelism());
    }
    return storedSource;
  }
}
