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
package edu.iu.dsc.tws.comms.table.ops;

import java.util.Set;

import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.comms.BaseOperation;
import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.common.table.Table;
import edu.iu.dsc.tws.comms.table.ArrowAllToAll;
import edu.iu.dsc.tws.comms.table.ArrowCallback;

public class TPartition extends BaseOperation {
  private ArrowAllToAll allToAll;

  /**
   * Create the base operation
   */
  public TPartition(Communicator comm, IWorkerController controller, Set<Integer> sources,
                    Set<Integer> targets, LogicalPlan plan, int edge,
                    Schema schema, ArrowCallback receiver) {
    super(comm, false, CommunicationContext.TABLE_PARTITION);
    this.allToAll = new ArrowAllToAll(comm.getConfig(), controller,
        sources, targets, edge, receiver, schema);
  }

  public boolean insert(int source, Table t, int destination) {
    return allToAll.insert(t, destination);
  }

  @Override
  public boolean isComplete() {
    return allToAll.isComplete();
  }

  @Override
  public void finish(int src) {
    allToAll.finish(src);
  }

  @Override
  public boolean progress() {
    return allToAll.isComplete();
  }

  @Override
  public void close() {
    allToAll.close();
  }

  @Override
  public void reset() {
    throw new Twister2RuntimeException("Not-implemented");
  }

  @Override
  public boolean progressChannel() {
    return allToAll.isComplete();
  }

  @Override
  public boolean sendBarrier(int src, byte[] barrierId) {
    throw new Twister2RuntimeException("Not-implemented");
  }

  @Override
  public void waitForCompletion() {
    super.waitForCompletion();
  }
}
