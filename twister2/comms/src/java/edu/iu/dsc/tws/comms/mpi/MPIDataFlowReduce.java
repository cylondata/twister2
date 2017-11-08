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
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.routing.IRouter;
import edu.iu.dsc.tws.comms.routing.SingleTargetBinaryTreeRouter;

public class MPIDataFlowReduce extends MPIDataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowBroadcast.class.getName());

  // the source tasks
  protected Set<Integer> sources;

  // the destination task
  protected int destination;

  protected IRouter router;

  public MPIDataFlowReduce(TWSMPIChannel channel, Set<Integer> sources, int destination) {
    super(channel);

    this.sources = sources;
    this.destination = destination;
  }

  public void setupRouting() {
    // we only have one path
    this.router = new SingleTargetBinaryTreeRouter(config, instancePlan,
        destination, sources);
  }

  @Override
  protected int destinationIdentifier() {
    return super.destinationIdentifier();
  }

  @Override
  protected boolean isLast(int source, int path, int taskIdentifier) {
    return router.isLast(taskIdentifier);
  }

  /**
   * We can receive messages from internal tasks or an external task, we allways receive messages
   * to the main task of the executor and we go from there
   *
   * @param currentMessage
   * @param object
   */
  @Override
  protected void receiveMessage(MPIMessage currentMessage, Object object) {
    MessageHeader header = currentMessage.getHeader();

    // we always receive to the main task
    int messageDestId = currentMessage.getHeader().getDestinationIdentifier();
    // check weather this message is for a sub task
    if (!isLast(header.getSourceId(), header.getPath(), messageDestId)
        && partialReceiver != null) {
      partialReceiver.onMessage(header.getSourceId(), header.getPath(),
          router.mainTaskOfExecutor(instancePlan.getThisExecutor()), object);
    } else {
      finalReceiver.onMessage(header.getSourceId(), header.getPath(),
          router.mainTaskOfExecutor(instancePlan.getThisExecutor()), object);
    }
  }

  @Override
  protected void routeReceivedMessage(MessageHeader message, List<Integer> routes) {
    throw new RuntimeException("We don't rout send received messages directly");
  }

  @Override
  protected void externalRoutesForSend(int source, List<Integer> routes) {
    // get the expected routes
    Map<Integer, Map<Integer, Set<Integer>>> routing = router.getExternalSendTasks(source);
    if (routing == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    Map<Integer, Set<Integer>> sourceRouting = routing.get(source);
    if (sourceRouting != null) {
      // we always use path 0 because only one path
      routes.addAll(sourceRouting.get(0));
    }
  }

  @Override
  protected void internalRoutesForSend(int source, List<Integer> routes) {
    // get the expected routes
    Map<Integer, Map<Integer, Set<Integer>>> routing = router.getInternalSendTasks(source);
    if (routing == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    Map<Integer, Set<Integer>> sourceRouting = routing.get(source);
    if (sourceRouting != null) {
      // we always use path 0 because only one path
      routes.addAll(sourceRouting.get(0));
    }
  }

  @Override
  protected void receiveSendInternally(int source, int t, int path, Object message) {
    // check weather this is the last task
    if (router.isLast(t)) {
      finalReceiver.onMessage(source, path, t, message);
    } else {
      partialReceiver.onMessage(source, path, t, message);
    }
  }

  @Override
  public void injectPartialResult(int source, Object message) {
    // now what we need to do

  }

  @Override
  protected Set<Integer> receivingExecutors() {
    return router.receivingExecutors();
  }

  @Override
  protected Map<Integer, Map<Integer, List<Integer>>> receiveExpectedTaskIds() {
    return router.receiveExpectedTaskIds();
  }
}
