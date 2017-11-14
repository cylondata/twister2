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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.routing.IRouter;

public class MPILoadBalance extends MPIDataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPILoadBalance.class.getName());

  private Random random;
  private Set<Integer> sources;
  private Set<Integer> destinations;

  protected Map<Integer, MPIMessage> currentMessages = new HashMap<>();

  protected IRouter router;

  public MPILoadBalance(TWSMPIChannel channel, Set<Integer> srcs, Set<Integer> dests) {
    super(channel);
    random = new Random(System.nanoTime());
    this.sources = srcs;
    this.destinations = dests;
  }

  protected void setupRouting() {

  }

  @Override
  protected void routeReceivedMessage(MessageHeader message, List<Integer> routes) {
    throw new RuntimeException("Load-balance doesn't rout received messages");
  }

  @Override
  public boolean injectPartialResult(int source, Object message) {
    throw new RuntimeException("Not supported method");
  }

  @Override
  protected void externalRoutesForSend(int source, List<Integer> routes) {
  }

  @Override
  protected void internalRoutesForSend(int source, List<Integer> routes) {
  }

  @Override
  protected void receiveSendInternally(int source, int t, int path, Object message) {

  }

  @Override
  protected Set<Integer> receivingExecutors() {
    return null;
  }

  @Override
  protected Map<Integer, Map<Integer, List<Integer>>> receiveExpectedTaskIds() {
    return null;
  }

  @Override
  protected boolean isLast(int source, int path, int taskIdentifier) {
    return false;
  }

  @Override
  protected void receiveMessage(MPIMessage currentMessage, Object object) {

  }

  @Override
  protected boolean isLastReceiver() {
    return false;
  }
}
