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
package edu.iu.dsc.tws.tset.links.batch.row;

import edu.iu.dsc.tws.api.tset.schema.TupleSchema;
import edu.iu.dsc.tws.api.tset.table.Row;
import edu.iu.dsc.tws.api.tset.table.TableSchema;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.env.CheckpointingTSetEnv;
import edu.iu.dsc.tws.tset.links.batch.BatchIteratorLink;
import edu.iu.dsc.tws.tset.sets.batch.row.RowCachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.row.RowCheckpointedTSet;
import edu.iu.dsc.tws.tset.sets.batch.row.RowPersistedTSet;
import edu.iu.dsc.tws.tset.sinks.DiskPersistIterSink;
import edu.iu.dsc.tws.tset.sources.DiskPartitionBackedSource;

public abstract class RowBatchIteratorLinkWrapper extends BatchIteratorLink<Row> {

  RowBatchIteratorLinkWrapper(BatchTSetEnvironment env, String n, int sourceP,
                                TupleSchema schema) {
    super(env, n, sourceP, schema);
  }

  RowBatchIteratorLinkWrapper(BatchTSetEnvironment env, String n, int sourceP, int targetP,
                                TupleSchema schema) {
    super(env, n, sourceP, targetP, schema);
  }

  protected RowBatchIteratorLinkWrapper() {
  }

  @Override
  public RowCachedTSet lazyCache() {
    // todo :: add sink function
    RowCachedTSet cacheTSet = new RowCachedTSet(getTSetEnv(), null,
        getTargetParallelism(), (TableSchema) getSchema());
    addChildToGraph(cacheTSet);

    return cacheTSet;
  }

  @Override
  public RowCachedTSet cache() {
    return (RowCachedTSet) super.cache();
  }

  @Override
  public RowPersistedTSet lazyPersist() {
    RowPersistedTSet persistedTSet = new RowPersistedTSet(getTSetEnv(),
        new DiskPersistIterSink<>(this.getId()), getTargetParallelism(), getSchema());
    addChildToGraph(persistedTSet);
    return persistedTSet;
  }

  @Override
  public RowPersistedTSet persist() {
    // handling checkpointing
    if (getTSetEnv().isCheckpointingEnabled()) {
      String persistVariableName = this.getId() + "-persisted";
      CheckpointingTSetEnv chkEnv = (CheckpointingTSetEnv) getTSetEnv();
      Boolean persisted = chkEnv.initVariable(persistVariableName, false);

      if (persisted) {
        // create a source function with the capability to read from disk
        DiskPartitionBackedSource<Row> sourceFn =
            new DiskPartitionBackedSource<>(this.getId());

        // pass the source fn to the checkpointed tset (that would create a source tset from the
        // source function, the same way as a persisted tset. This preserves the order of tsets
        // that are being created in the checkpointed env)
        RowCheckpointedTSet checkTSet = new RowCheckpointedTSet(getTSetEnv(), sourceFn,
            this.getTargetParallelism(), getSchema());

        // adding checkpointed tset to the graph, so that the IDs would not change
        addChildToGraph(checkTSet);

        // run only the checkpointed tset so that it would populate the inputs in the executor
        getTSetEnv().runOne(checkTSet);

        return checkTSet;
      } else {
        RowPersistedTSet storable = this.doPersist();
        chkEnv.updateVariable(persistVariableName, true);
        chkEnv.commit();
        return storable;
      }
    }
    return doPersist();
  }

  @Override
  protected TableSchema getSchema() {
    return (TableSchema) super.getSchema();
  }

  private RowPersistedTSet doPersist() {
    RowPersistedTSet lazyPersist = lazyPersist();
    getTSetEnv().run(lazyPersist);
    return lazyPersist;
  }
}
