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
package edu.iu.dsc.tws.comms.mpi;

import java.util.HashMap;
import java.util.Map;

public abstract class MPIGroupedDataFlowOperation extends MPIDataFlowOperation {
  /**
   * Keep track of the current message been received
   */
  protected Map<Integer, Map<Integer, MPIMessage>> groupedCurrentMessages = new HashMap<>();


  public MPIGroupedDataFlowOperation(TWSMPIChannel channel) {
    super(channel);
  }

  @Override
  protected void initSerializers() {
    messageDeSerializer.init(config, true);
    messageSerializer.init(config, true);
  }

  @Override
  public void onReceiveComplete(int id, int e, MPIBuffer buffer) {
    // we need to get the path at which this message is sent
    int path = buffer.getByteBuffer().getInt();
    // get the message map according to the path
    Map<Integer, MPIMessage> messageMap = groupedCurrentMessages.get(path);

    // we need to try to build the message here, we may need many more messages to complete
    MPIMessage currentMessage = messageMap.get(id);
    if (currentMessage == null) {
      currentMessage = new MPIMessage(type, MPIMessageDirection.IN, this);
      messageMap.put(id, currentMessage);
    }

    Object object = messageDeSerializer.buid(buffer, currentMessage, e);

    // if the message is complete, send it further down and call the receiver
    if (currentMessage.isComplete()) {
      // we may need to pass this down to others
      passMessageDownstream(currentMessage);
      // we received a message, we need to determine weather we need to
      // forward to another node and process
      receiver.onMessage(object);
      // okay we built this message, lets remove it from the map
      messageMap.remove(id);
    }
  }
}
