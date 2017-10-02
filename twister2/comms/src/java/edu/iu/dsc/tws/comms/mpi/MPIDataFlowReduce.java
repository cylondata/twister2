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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.Message;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.routing.IRouter;

public class MPIDataFlowReduce extends MPIDataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowBroadcast.class.getName());

  /**
   * Keep track of the current message been received
   */
  private Map<Integer, Map<Integer, MPIMessage>> currentMessages = new HashMap<>();

  public MPIDataFlowReduce(TWSMPIChannel channel) {
    super(channel);
  }

  public IRouter setupRouting() {
    return null;
  }

  @Override
  protected void routeMessage(MessageHeader message, List<Integer> routes) {

  }

  @Override
  public void onReceiveComplete(int id, int stream, MPIBuffer buffer) {
    int originatingNode = buffer.getByteBuffer().getInt();
    int sourceNode = buffer.getByteBuffer().getInt();

    Map<Integer, MPIMessage> messageMap = currentMessages.get(sourceNode);

    // we need to try to build the message here, we may need many more messages to complete
    MPIMessage currentMessage = messageMap.get(originatingNode);

    if (currentMessage == null) {
      MessageHeader header = buildHeader(buffer);
      currentMessage = new MPIMessage(thisTask, header, MPIMessageType.RECEIVE, this);
      messageMap.put(originatingNode, currentMessage);
    } else if (!currentMessage.isComplete()) {
      currentMessage.addBuffer(buffer);
      currentMessage.build();
    }

    if (currentMessage.isComplete()) {
      List<Integer> routes = new ArrayList<>();
      // we will get the routing based on the originating id
      routeMessage(currentMessage.getHeader(), routes);
      // try to send further
      sendMessage(currentMessage, routes);

      // we received a message, we need to determine weather we need to
      // forward to another node and process
      if (messageDeSerializer != null) {
        Object object = messageDeSerializer.buid(currentMessage);
        receiver.onMessage(object);
      }

      currentMessages.remove(originatingNode);
    }
  }

  @Override
  public void sendPartial(Message message) {
    throw new UnsupportedOperationException("partial messages not supported by reduce");
  }

  @Override
  public void finish() {
    throw new UnsupportedOperationException("partial messages not supported by reduce");
  }

  @Override
  public void sendComplete(Message message) {

  }
}
