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

import java.util.List;
import java.util.Set;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.Message;
import edu.iu.dsc.tws.comms.api.MessageDeSerializer;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageSerializer;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.routing.IRouter;

public class MPIDataFlowAllReduce extends MPIDataFlowOperation {
  public MPIDataFlowAllReduce(TWSMPIChannel channel) {
    super(channel);
  }

  @Override
  public void release(MPIMessage message) {

  }

  @Override
  public void onReceiveComplete(int id, int stream, MPIBuffer message) {

  }

  @Override
  public void onSendComplete(int id, int stream, MPIMessage message) {

  }

  @Override
  public void init(Config config, int thisTask, TaskPlan instancePlan,
                   Set<Integer> sources, Set<Integer> destinations,
                   int stream, MessageReceiver receiver,
                   MessageDeSerializer messageDeSerializer,
                   MessageSerializer messageSerializer,
                   MessageReceiver partialRcvr) {

  }

  @Override
  public void sendPartialMessage(Message message) {

  }

  @Override
  public void finish() {

  }

  @Override
  protected IRouter setupRouting() {
    return null;
  }

  @Override
  protected void routeReceivedMessage(MessageHeader message, List<Integer> routes) {

  }

  @Override
  protected void routeSendMessage(MessageHeader message, List<Integer> routes) {

  }


  @Override
  public void sendCompleteMessage(Message message) {

  }

  @Override
  public void close() {

  }
}
