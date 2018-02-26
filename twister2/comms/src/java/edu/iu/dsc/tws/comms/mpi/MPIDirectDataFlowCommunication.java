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
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
<<<<<<< HEAD
import java.util.concurrent.atomic.AtomicBoolean;
=======
import java.util.logging.Level;
>>>>>>> 512841c60233c3fe52b637f798e0d788ecbe5e00
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.MPIMessageDeSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MPIMessageSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MessageDeSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MessageSerializer;
import edu.iu.dsc.tws.comms.routing.DirectRouter;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

/**
 * A direct data flow operation sends peer to peer messages
 */
public class MPIDirectDataFlowCommunication implements DataFlowOperation, MPIMessageReceiver {
  private static final Logger LOG = Logger.getLogger(
      MPIDirectDataFlowCommunication.class.getName());
  private Set<Integer> sources;
  private int destination;
  private DirectRouter router;
  private MessageReceiver finalReceiver;
  private MPIDataFlowOperation delegete;
  private Config config;
  private TaskPlan instancePlan;
  private int executor;
  private MessageType type;
  private AtomicBoolean finalReceiverProgress;

  public MPIDirectDataFlowCommunication(TWSChannel channel,
                                        Set<Integer> srcs, int dest,
                                        MessageReceiver finalRcvr) {
    this.sources = srcs;
    this.destination = dest;
    this.finalReceiver = finalRcvr;
    this.delegete = new MPIDataFlowOperation(channel);
    this.finalReceiverProgress = new AtomicBoolean(false);
  }

  @Override
  public boolean receiveMessage(MPIMessage currentMessage, Object object) {
    MessageHeader header = currentMessage.getHeader();
    LOG.info("================================================");
    LOG.info("MessageHeader : " + header.toString());
    LOG.info("MPIMessage : " + currentMessage.toString());
    LOG.info("Message Object : " + object.toString());
    LOG.info("Source ID : " + header.getSourceId());
    LOG.info("Source ID : " + header.getSourceId());
    LOG.info("================================================");

    // check weather this message is for a sub task
<<<<<<< HEAD
    return finalReceiver.onMessage(
        header.getSourceId(), MPIContext.DEFAULT_PATH,
=======
    return finalReceiver.onMessage(header.getSourceId(), 0,
>>>>>>> 512841c60233c3fe52b637f798e0d788ecbe5e00
        destination, header.getFlags(), object);
  }

  @Override
  public boolean receiveSendInternally(int source, int t, int path, int flags, Object message) {
    // we only have one destination in this case
    if (t != destination) {
      throw new RuntimeException("We only have one destination");
    }

    // okay this must be for the
    return finalReceiver.onMessage(source, path, t, flags, message);
  }

  @Override
  public boolean passMessageDownstream(Object object, MPIMessage currentMessage) {
    return false;
  }

  protected Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    return this.router.receiveExpectedTaskIds();
  }

  /**
   * Initialize
   * @param cfg
   * @param t
   * @param taskPlan
   * @param edge
   */
  public void init(Config cfg, MessageType t, TaskPlan taskPlan, int edge) {

    this.config = cfg;
    this.instancePlan = taskPlan;
    this.type = t;
    this.router = new DirectRouter(taskPlan, sources, destination); // assume working:vibhatha

    if (this.finalReceiver != null) { // isLastReciever ??? : vibhatha
      LG.info("====================================================");
      LG.info("Final Recieve Null Check and IsLastReceiverCheck");
      LG.info("====================================================");
      this.finalReceiver.init(cfg, this, receiveExpectedTaskIds());
    } else {
      LG.info("Final Receiver Not Called");
      //throw new RuntimeException("Final receiver is required");
    }

    Map<Integer, ArrayBlockingQueue<Pair<Object, MPISendMessage>>> pendingSendMessagesPerSource =
        new HashMap<>();
    Map<Integer, Queue<Pair<Object, MPIMessage>>> pendingReceiveMessagesPerSource = new HashMap<>();
    Map<Integer, Queue<MPIMessage>> pendingReceiveDeSerializations = new HashMap<>();
    Map<Integer, MessageSerializer> serializerMap = new HashMap<>();
    Map<Integer, MessageDeSerializer> deSerializerMap = new HashMap<>();
    /***
     * Original Code Start
     * */
    Set<Integer> srcs = TaskPlanUtils.getTasksOfThisExecutor(taskPlan, sources);
    /*for (int s : srcs) {
      // later look at how not to allocate pairs for this each time
      ArrayBlockingQueue<Pair<Object, MPISendMessage>> pendingSendMessages =
          new ArrayBlockingQueue<Pair<Object, MPISendMessage>>(
              MPIContext.sendPendingMax(config));
      pendingSendMessagesPerSource.put(s, pendingSendMessages);
      pendingReceiveDeSerializations.put(s, new ArrayBlockingQueue<MPIMessage>(
          MPIContext.sendPendingMax(config)));
      serializerMap.put(s, new MPIMessageSerializer(new KryoSerializer()));
    }*/
    /*LG.info("++++++++++++++++++++++++++++++++++++++++++++++");
    LG.info("pendingReceiveDeSerializations size : " + pendingReceiveDeSerializations.size());
    if (pendingReceiveDeSerializations.size() > 0) {
      LG.info("Message Content  pendingReceiveDeSerializations : "
      + pendingReceiveDeSerializations.get(0).poll());
    }
    LG.info("++++++++++++++++++++++++++++++++++++++++++++++");*/

    /***
     * Original Code Ends
     * */


    /**
     * Section Start : Testing Direct
     * */

    Set<Integer> myexecutors = router.receivingExecutors();
    Set<Integer> mysources = router.allSendTasks();

    LG.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
    LG.info("Sources : " + mysources.size());
    LG.info("Executors : " + myexecutors.size());
    if (mysources.iterator().hasNext()) {
      LG.info("Source 1 : " + mysources.iterator().next());
    }
    if (myexecutors.iterator().hasNext()) {
      LG.info("Executor 1 : " + myexecutors.iterator().next());
    }

    for (int s : mysources) {
      // later look at how not to allocate pairs for this each time
      ArrayBlockingQueue<Pair<Object, MPISendMessage>> pendingSendMessages =
          new ArrayBlockingQueue<Pair<Object, MPISendMessage>>(
              MPIContext.sendPendingMax(cfg));
      pendingSendMessagesPerSource.put(s, pendingSendMessages);
      serializerMap.put(s, new MPIMessageSerializer(new KryoSerializer()));
    }

    int maxReceiveBuffers = MPIContext.receiveBufferCount(cfg);
    int receiveExecutorsSize = myexecutors.size();
    if (receiveExecutorsSize == 0) {
      receiveExecutorsSize = 1;
    }

    Set<Integer> execs = router.receivingExecutors();
    for (int e : execs) {
      int capacity = maxReceiveBuffers * 2 * receiveExecutorsSize;
      LG.info("Capacity : " + capacity);
      Queue<Pair<Object, MPIMessage>> pendingReceiveMessages =
          new ArrayBlockingQueue<Pair<Object, MPIMessage>>(
              capacity);
      pendingReceiveMessagesPerSource.put(e, pendingReceiveMessages);
      pendingReceiveDeSerializations.put(e, new ArrayBlockingQueue<MPIMessage>(capacity));
      deSerializerMap.put(e, new MPIMessageDeSerializer(new KryoSerializer()));
    }

    MPIMessage mpimsg = pendingReceiveDeSerializations.get(0).poll();
    LG.info("MPI Message : " + mpimsg);

    KryoSerializer kryoSerializer = new KryoSerializer();
    kryoSerializer.init(new HashMap<String, Object>());

    MessageDeSerializer messageDeSerializer = new MPIMessageDeSerializer(kryoSerializer);
    MessageSerializer messageSerializer = new MPIMessageSerializer(kryoSerializer);

    delegete.init(cfg, t, taskPlan, edge,
        router.receivingExecutors(), router.isLastReceiver(), this,
        pendingSendMessagesPerSource, pendingReceiveMessagesPerSource,
        pendingReceiveDeSerializations, serializerMap, deSerializerMap, false);

    LG.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");

    /**
     * Section End : Testing Direct
     * */

    /***
     * Original Code Starts
     * */

   /* MessageDeSerializer messageDeSerializer = new MPIMessageDeSerializer(new KryoSerializer());
    deSerializerMap.put(destination, messageDeSerializer);
    delegete.init(cfg, t, taskPlan, edge, router.receivingExecutors(),
<<<<<<< HEAD
        isLastReceiver(), this, pendingSendMessagesPerSource, pendingReceiveMessagesPerSource,
        pendingReceiveDeSerializations, serializerMap, deSerializerMap, false);*/

    /***
     * Original Code Ends
     * */
=======
        isLastReceiver(), this, pendingSendMessagesPerSource,
        pendingReceiveMessagesPerSource,
        pendingReceiveDeSerializations, serializerMap, deSerializerMap, false);
>>>>>>> 512841c60233c3fe52b637f798e0d788ecbe5e00
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    throw new RuntimeException("This method is not used by direct communication");
  }

  @Override
  public boolean send(int source, Object message, int flags) {
<<<<<<< HEAD
    /*LG.info("================================================");
    LG.info("Source : " + source);
    LG.info("Message : " + message.toString());
    LG.info("Flags : " + flags);
    LG.info("================================================");*/
=======
    LOG.info("================================================");
    LOG.info("Source : " + source);
    LOG.info("Message : " + message.toString());
    LOG.info("Flags : " + flags);
    LOG.info("================================================");
>>>>>>> 512841c60233c3fe52b637f798e0d788ecbe5e00
    return delegete.sendMessage(source, message, 0, flags, sendRoutingParameters(source, 0));
  }

  @Override
  public boolean send(int source, Object message, int flags, int dest) {
    return delegete.sendMessage(source, message, dest, flags,
        sendRoutingParameters(source, dest));
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int dest) {
    return false;
  }

  @Override
  public void progress() {
<<<<<<< HEAD
    delegete.progress();
    finalReceiver.progress();
    /*if (finalReceiverProgress.compareAndSet(false, true)) {
      finalReceiver.progress();
      finalReceiverProgress.compareAndSet(true, false);
    }*/
=======
    try {
      delegete.progress();
      finalReceiver.progress();
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "un-expected error", t);
      throw new RuntimeException(t);
    }
>>>>>>> 512841c60233c3fe52b637f798e0d788ecbe5e00
  }

  @Override
  public void close() {
  }

  @Override
  public void finish() {
  }

  @Override
  public MessageType getType() {
    return null;
  }

  @Override
  public TaskPlan getTaskPlan() {
    return null;
  }

  @Override
  public void setMemoryMapped(boolean memoryMapped) {
    delegete.setStoreBased(memoryMapped);
  }

  private boolean isLastReceiver() {
    return router.isLastReceiver();
  }

  private RoutingParameters sendRoutingParameters(int source, int path) {
    RoutingParameters routingParameters = new RoutingParameters();
    // get the expected routes
    Map<Integer, Set<Integer>> internalRoutes = router.getInternalSendTasks(source);
    if (internalRoutes == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    Set<Integer> internalSourceRouting = internalRoutes.get(source);
    if (internalSourceRouting != null) {
      // we always use path 0 because only one path
      routingParameters.addInternalRoutes(internalSourceRouting);
    }

    // get the expected routes
    Map<Integer, Set<Integer>> externalRouting = router.getExternalSendTasks(source);
    if (externalRouting == null) {
      throw new RuntimeException("Un-expected message from source: " + source);
    }

    Set<Integer> externalSourceRouting = externalRouting.get(source);
    if (externalSourceRouting != null) {
      // we always use path 0 because only one path
      routingParameters.addExternalRoutes(externalSourceRouting);
    }
    routingParameters.setDestinationId(destination);
    return routingParameters;
  }
}
