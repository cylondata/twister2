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
import edu.iu.dsc.tws.common.table.Row;
import edu.iu.dsc.tws.api.tset.schema.RowSchema;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;

public class RowCachedTSet extends RowStoredTSet {
  public RowCachedTSet(BatchTSetEnvironment tSetEnv, SinkFunc<Row> sinkFunc,
                         int parallelism, RowSchema inputSchema) {
    super(tSetEnv, "kcached", sinkFunc, parallelism, inputSchema);
  }

  @Override
  public RowCachedTSet setName(String n) {
    return (RowCachedTSet) super.setName(n);
  }

  public RowCachedTSet withSchema(TupleSchema schema) {
    return (RowCachedTSet) super.withSchema((RowSchema) schema);
  }

  @Override
  public RowCachedTSet cache() {
    return this;
  }

  @Override
  public RowCachedTSet lazyCache() {
    return this;
  }

  @Override
  public RowCachedTSet persist() {
    throw new UnsupportedOperationException("persist on CachedTSet is undefined!");
  }

  @Override
  public RowCachedTSet lazyPersist() {
    throw new UnsupportedOperationException("lazyPersist on CachedTSet is undefined!");
  }

  @Override
  public INode getINode() {
    return null;
  }
}
