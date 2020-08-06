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
import edu.iu.dsc.tws.api.tset.schema.RowSchema;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.links.TLinkUtils;

public class RowPipeTLink extends RowBatchTLinkImpl {
  private boolean useDisk = false;

  private RowPipeTLink() {
    //non arg constructor for kryp
  }

  public RowPipeTLink(BatchEnvironment tSetEnv, int sourceParallelism, RowSchema schema) {
    super(tSetEnv, "direct", sourceParallelism, schema);
  }

  public RowPipeTLink(BatchEnvironment tSetEnv, String name, int sourceParallelism,
                       RowSchema schema) {
    super(tSetEnv, name, sourceParallelism, schema);
  }

  @Override
  public RowPipeTLink setName(String name) {
    rename(name);
    return this;
  }

  @Override
  public Edge getEdge() {
    Edge e = new Edge(getId(), OperationNames.TABLE_PIPE, MessageTypes.ARROW_TABLE);
    e.addProperty(CommunicationContext.USE_DISK, this.useDisk);
    TLinkUtils.generateCommsSchema(getSchema(), e);
    return e;
  }

  public RowPipeTLink useDisk() {
    this.useDisk = true;
    return this;
  }
}
