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

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.OperationNames;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.table.Row;
import edu.iu.dsc.tws.api.tset.table.RowSchema;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.links.TLinkUtils;

public class RowPartitionTLink extends RowBatchTLinkImpl {
  private boolean useDisk = false;

  private PartitionFunc<Row> partitionFunction;

  public RowPartitionTLink(BatchTSetEnvironment tSetEnv,
                           int sourceParallelism, RowSchema schema) {
    this(tSetEnv, null, sourceParallelism, schema);
  }

  public RowPartitionTLink(BatchTSetEnvironment tSetEnv, PartitionFunc<Row> parFn,
                        int sourceParallelism, RowSchema schema) {
    this(tSetEnv, parFn, sourceParallelism, sourceParallelism, schema);
  }

  public RowPartitionTLink(BatchTSetEnvironment tSetEnv, PartitionFunc<Row> parFn,
                        int sourceParallelism, int targetParallelism, RowSchema schema) {
    super(tSetEnv, "partition", sourceParallelism, targetParallelism, schema);
    this.partitionFunction = parFn;
  }

  @Override
  public Edge getEdge() {
    Edge e = new Edge(getId(), OperationNames.TABLE_PARTITION, MessageTypes.ARROW_TABLE);
    if (partitionFunction != null) {
      e.setPartitioner(partitionFunction);
    }
    e.addProperty(CommunicationContext.USE_DISK, this.useDisk);
    TLinkUtils.generateCommsSchema(getSchema(), e);
    return e;
  }

  public RowPartitionTLink useDisk() {
    this.useDisk = true;
    return this;
  }

  @Override
  public TBase setName(String name) {
    rename(name);
    return this;
  }
}
