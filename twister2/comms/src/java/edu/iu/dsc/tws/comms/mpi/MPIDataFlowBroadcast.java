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
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageDeSerializer;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageSerializer;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.routing.BinaryTreeRouter;
import edu.iu.dsc.tws.comms.routing.IRouter;
import edu.iu.dsc.tws.comms.routing.Routing;

public class MPIDataFlowBroadcast extends MPIDataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowBroadcast.class.getName());
  /**
   * Keep track of the current message been received
   */
  private Map<Integer, Map<Integer, MPIMessage>> currentMessages = new HashMap<>();


  public MPIDataFlowBroadcast(TWSMPIChannel channel) {
    super(channel);
  }

  @Override
  public void init(Config cfg, MessageType messageType, int task, TaskPlan plan,
                   Set<Integer> srcs, Set<Integer> dests,
                   int messageStream, MessageReceiver rcvr,
                   MessageDeSerializer fmtr, MessageSerializer bldr,
                   MessageReceiver partialRcvr) {
    super.init(cfg, messageType, task, plan, srcs, dests,
        messageStream, rcvr, fmtr, bldr, partialRcvr);

    // broadcast only supports a single source
    if (sources.size() > 1) {
      throw new RuntimeException("Broadcast only supports one source");
    }

    for (Integer source : expectedRoutes.keySet()) {
      currentMessages.put(source, new HashMap<Integer, MPIMessage>());
    }
  }

  @Override
  protected void sendCompleteMPIMessage(MPIMessage mpiMessage) {
    List<Integer> routes = new ArrayList<>();
    routeSendMessage(mpiMessage.getHeader(), routes);
    if (routes.size() == 0) {
      throw new RuntimeException("Failed to get downstream tasks");
    }
    sendMessage(mpiMessage, routes);
  }

  @Override
  public void close() {
  }

  protected void passMessageDownstream(MPIMessage currentMessage) {
    List<Integer> routes = new ArrayList<>();
    // we will get the routing based on the originating id
    routeReceivedMessage(currentMessage.getHeader(), routes);
    // try to send further
    sendMessage(currentMessage, routes);
  }

  protected IRouter setupRouting() {
    // lets create the routing needed
    BinaryTreeRouter tree = new BinaryTreeRouter();
    // we will only have one distinct route
    tree.init(config, thisTask, instancePlan, sources, destinations, edge, 1);
    return tree;
  }

  @Override
  protected void routeReceivedMessage(MessageHeader message, List<Integer> routes) {
    // check the origin
    int source = message.getSourceId();
    // get the expected routes
    Routing routing = expectedRoutes.get(source);

    if (routing == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    routes.addAll(routing.getDownstreamIds());
  }

  @Override
  protected void routeSendMessage(MessageHeader message, List<Integer> routes) {
    // check the origin
    int source = message.getSourceId();
    // get the expected routes
    Routing routing = expectedRoutes.get(source);

    if (routing == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    routes.addAll(routing.getDownstreamIds());
  }
}

