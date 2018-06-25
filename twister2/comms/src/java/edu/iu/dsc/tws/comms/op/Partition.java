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
package edu.iu.dsc.tws.comms.op;

import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.dfw.DataFlowContext;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;

public class Partition {
  private static final Logger LOG = Logger.getLogger(Partition.class.getName());

  private DataFlowPartition partition;

  private Communicator comm;

  public Partition(Communicator comm) {
    this.comm = comm;
  }

  public void partition(int source, Object message) {
    if (!comm.getSources().contains(source)) {
      throw new RuntimeException("Source is not in the communicator");
    }
    partition.send(source, message, 0, DataFlowContext.DEFAULT_DESTINATION);
  }

  public void keyedPartition(int source, Object message, int destination) {

  }

  public void finish(int source) {
    partition.finish(source);
  }
}
