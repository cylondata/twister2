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
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.schema.TupleSchema;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.api.tset.table.Row;
import edu.iu.dsc.tws.api.tset.table.RowSchema;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;

public class RowPersistedTSet extends RowStoredTSet {

  public RowPersistedTSet(BatchTSetEnvironment tSetEnv,
                          SinkFunc<Row> storingSinkFn, int parallelism,
                          RowSchema inputSchema) {
    super(tSetEnv, "kpersisted", storingSinkFn, parallelism, inputSchema);
  }

  @Override
  public RowPersistedTSet setName(String n) {
    return (RowPersistedTSet) super.setName(n);
  }

  @Override
  public RowPersistedTSet addInput(String key, StorableTBase<?> input) {
    return (RowPersistedTSet) super.addInput(key, input);
  }

  public RowPersistedTSet withSchema(TupleSchema schema) {
    return (RowPersistedTSet) super.withSchema((RowSchema) schema);
  }

  @Override
  public RowCachedTSet cache() {
    throw new UnsupportedOperationException("Cache on PersistedTSet is undefined!");
  }

  @Override
  public RowCachedTSet lazyCache() {
    throw new UnsupportedOperationException("Cache on PersistedTSet is undefined!");
  }

  @Override
  public RowPersistedTSet persist() {
    return this;
  }

  @Override
  public RowPersistedTSet lazyPersist() {
    return this;
  }

  @Override
  public INode getINode() {
    return null;
  }
}
