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

import edu.iu.dsc.tws.api.comms.BaseOperation;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.comms.table.ArrowTable;

public class TPartition extends BaseOperation {
  /**
   * Create the base operation
   */
  public TPartition(Communicator comm, boolean stream, String opName) {
    super(comm, stream, opName);
  }

  public boolean insert(int source, ArrowTable t, int destination) {
    return true;
  }

  @Override
  public boolean isComplete() {
    return super.isComplete();
  }

  @Override
  public void finish(int src) {
    super.finish(src);
  }

  @Override
  public boolean progress() {
    return super.progress();
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  public void reset() {
    super.reset();
  }

  @Override
  public boolean progressChannel() {
    return super.progressChannel();
  }

  @Override
  public boolean sendBarrier(int src, byte[] barrierId) {
    return super.sendBarrier(src, barrierId);
  }

  @Override
  public void waitForCompletion() {
    super.waitForCompletion();
  }
}
