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
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.Message;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.routing.BinaryTreeRouter;
import edu.iu.dsc.tws.comms.routing.IRouter;

public class MPIKeyedReduce extends MPIDataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPIKeyedReduce.class.getName());

  public MPIKeyedReduce(TWSMPIChannel channel) {
    super(channel);
  }

  @Override
  public void onReceiveComplete(int id, int stream, MPIBuffer message) {

  }

  @Override
  public void sendCompleteMessage(Message message) {

  }

  @Override
  protected IRouter setupRouting() {
    // lets create the routing needed
    BinaryTreeRouter tree = new BinaryTreeRouter();
    tree.init(config, thisTask, instancePlan, destinations, sources, stream,
        MPIContext.distinctRoutes(config, sources.size()));
    return tree;
  }

  @Override
  protected void routeReceivedMessage(MessageHeader message, List<Integer> routes) {

  }

  @Override
  protected void routeSendMessage(MessageHeader message, List<Integer> routes) {

  }
}
