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

import java.util.Comparator;
import java.util.List;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.link.batch.BatchRowTLink;
import edu.iu.dsc.tws.api.tset.schema.RowSchema;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchRowTSet;
import edu.iu.dsc.tws.common.table.Row;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.ops.row.RowSinkOp;
import edu.iu.dsc.tws.tset.sources.DataPartitionSourceFunc;

public abstract class RowStoredTSet extends BatchRowTSetImpl implements StorableTBase<Row> {
  // batch keyed comms only output iterators
  private String storedSourcePrefix;
  private SinkFunc<Row> storingSinkFunc;
  protected RowSourceTSet storedSource;

  RowStoredTSet(BatchTSetEnvironment tSetEnv, String name,
                  SinkFunc<Row> storingSinkFn, int parallelism,
                  RowSchema inputSchema) {
    super(tSetEnv, name, parallelism, inputSchema);
    this.storingSinkFunc = storingSinkFn;
    this.storedSourcePrefix = "kstored(" + getId() + ")";
  }

  @Override
  public BatchRowTLink partition(PartitionFunc<Row> partitionFn,
                                                     int targetParallelism, int columnIndex) {
    return getStoredSourceTSet().partition(partitionFn, targetParallelism, columnIndex);
  }

  @Override
  public BatchRowTLink join(BatchRowTSet rightTSet,
                                             CommunicationContext.JoinType type,
                                             Comparator<Row> keyComparator) {
    return getStoredSourceTSet().join(rightTSet, type, keyComparator);
  }

  @Override
  public BatchRowTLink direct() {
    return getStoredSourceTSet().direct();
  }

  @Override
  public List<Row> getData() {
    throw new RuntimeException("Not implemented yet");
  }

  @Override
  public RowSourceTSet getStoredSourceTSet() {
    if (this.storedSource == null) {
      // this cache source will consume the data object created by the execution of this tset.
      // hence this tset ID needs to be set as an input to the cache source
      this.storedSource = getTSetEnv().createRowSource(storedSourcePrefix,
          new DataPartitionSourceFunc<>(storedSourcePrefix), getParallelism());
      this.storedSource.addInput(storedSourcePrefix, this);
    }

    return this.storedSource;
  }

  @Override
  public RowSchema getInputSchema() {
    return (RowSchema) super.getInputSchema();
  }

  @Override
  public INode getINode() {
    return new RowSinkOp(storingSinkFunc, this, getInputs());
  }
}
