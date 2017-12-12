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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageDeSerializer;
import edu.iu.dsc.tws.comms.api.MessageSerializer;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.KryoSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MPIMessageDeSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MPIMessageSerializer;

public abstract class MPIDataFlowOperation implements DataFlowOperation,
    MPIMessageListener, MPIMessageReleaseCallback {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowOperation.class.getName());

  // the configuration
  protected Config config;
  // the task plan
  protected TaskPlan instancePlan;

  protected int edge;
  // the router that gives us the possible routes
  protected TWSMPIChannel channel;
  protected MessageDeSerializer messageDeSerializer;
  protected MessageSerializer messageSerializer;
  // we may have multiple routes throughus

  protected MessageType type;
  protected int executor;
  private int sendCount = 0;
  private int sendCountFull = 0;
  /**
   * The send sendBuffers used by the operation
   */
  protected Queue<MPIBuffer> sendBuffers;

  /**
   * Receive availableBuffers, for each receive we need to make
   */
  protected Map<Integer, Queue<MPIBuffer>> receiveBuffers;

  /**
   * Pending send messages
   */
//  protected Queue<Pair<Object, MPISendMessage>> pendingSendMessages;
  protected Map<Integer, ArrayBlockingQueue<Pair<Object, MPISendMessage>>>
      pendingSendMessagesPerSource;

  /**
   * Pending receives in case the receives are not ready
   */
  protected Map<Integer, Queue<Pair<Object, MPIMessage>>> pendingReceiveMessagesPerSource;
//  protected Queue<Pair<Object, MPIMessage>> pendingReceiveMessages;

  /**
   * Non grouped current messages
   */
  private Map<Integer, MPIMessage> currentMessages = new HashMap<>();

  protected KryoSerializer kryoSerializer;

  /**
   * A lock to serialize access to the resources
   */
  protected final Lock lock = new ReentrantLock();

  public MPIDataFlowOperation(TWSMPIChannel channel) {
    this.channel = channel;
  }

  @Override
  public void init(Config cfg, MessageType messageType, TaskPlan plan, int graphEdge) {
    this.config = cfg;
    this.instancePlan = plan;
    this.edge = graphEdge;
    this.type = messageType;
    this.executor = instancePlan.getThisExecutor();

    int noOfSendBuffers = MPIContext.broadcastBufferCount(config);
    int sendBufferSize = MPIContext.bufferSize(config);

    this.sendBuffers = new ArrayBlockingQueue<MPIBuffer>(noOfSendBuffers);
    for (int i = 0; i < noOfSendBuffers; i++) {
      sendBuffers.offer(new MPIBuffer(sendBufferSize));
    }
    this.receiveBuffers = new HashMap<>();
    this.pendingSendMessagesPerSource = new HashMap<>();
    this.pendingReceiveMessagesPerSource = new HashMap<>();

    LOG.info(String.format("%d setup routing", instancePlan.getThisExecutor()));
    // this should setup a router
    setupRouting();

    LOG.info(String.format("%d setup communication", instancePlan.getThisExecutor()));
    // now setup the sends and receives
    setupCommunication();

    // initialize the serializers
    LOG.info(String.format("%d setup intializers", instancePlan.getThisExecutor()));
    initSerializers();
  }

  protected void initSerializers() {
    kryoSerializer = new KryoSerializer();
    kryoSerializer.init(new HashMap<String, Object>());

    messageDeSerializer = new MPIMessageDeSerializer(kryoSerializer);
    messageSerializer = new MPIMessageSerializer(sendBuffers, kryoSerializer);
    // initialize the serializers
    messageSerializer.init(config, false);
    messageDeSerializer.init(config, false);
  }

  protected abstract void setupRouting();
  protected abstract boolean isLastReceiver();

  protected RoutingParameters partialSendRoutingParameters(int source, int path) {
    throw new RuntimeException("This method needs to be implemented by the specific operation");
  }

  protected abstract RoutingParameters sendRoutingParameters(int source, int path);

  /**
   * Receive the sends internally directly
   * @param t
   * @param message
   */
  protected abstract boolean receiveSendInternally(int source, int t, int path, Object message);

  /**
   * Return the list of receiving executors for this
   * @return
   */
  protected abstract Set<Integer> receivingExecutors();

  /**
   * Initialize the message receiver with tasks from which messages are expected
   * For each sub edge in graph, for each path, gives the expected task ids
   *
   * path -> (ids)
   *
   * @return  expected task ids
   */
//  protected abstract Map<Integer, Map<Integer, List<Integer>>> receiveExpectedTaskIds();

  /**
   * Is this the final task
   * @return
   */
  protected abstract boolean isLast(int source, int path, int taskIdentifier);


  protected abstract boolean receiveMessage(MPIMessage currentMessage, Object object);

  /**
   * By default we are not doing anything here and the specific operations can override this
   *
   * @param currentMessage
   */
  protected boolean passMessageDownstream(Object object, MPIMessage currentMessage) {
    return true;
  }

  @Override
  public boolean send(int source, Object message, int path) {
    return false;
  }

  @Override
  public boolean sendPartial(int source, Object message, int path) {
    return false;
  }

  /**
   * Setup the receives and send sendBuffers
   */
  private void setupCommunication() {
    // we will receive from these
    int maxReceiveBuffers = MPIContext.receiveBufferCount(config);
    int receiveBufferSize = MPIContext.bufferSize(config);
    for (Integer recv : receivingExecutors()) {
      Queue<MPIBuffer> recvList = new LinkedList<>();
      for (int i = 0; i < maxReceiveBuffers; i++) {
        recvList.add(new MPIBuffer(receiveBufferSize));
      }
      // register with the channel
      LOG.info(instancePlan.getThisExecutor() + " Register to receive from: " + recv);
      channel.receiveMessage(recv, edge, this, recvList);
      receiveBuffers.put(recv, recvList);
    }

    // configure the send sendBuffers
    int sendBufferSize = MPIContext.bufferSize(config);
    int sendBufferCount = MPIContext.sendBuffersCount(config);
    for (int i = 0; i < sendBufferCount; i++) {
      MPIBuffer buffer = new MPIBuffer(sendBufferSize);
      sendBuffers.offer(buffer);
    }
  }

  /**
   * Sends a complete message
   * @param message the message object
   */
  @Override
  public boolean send(int source, Object message) {
    return sendMessage(source, message, MPIContext.DEFAULT_PATH);
  }

  protected boolean sendMessagePartial(int source, Object object, int path) {
    lock.lock();
    try {
      ArrayBlockingQueue<Pair<Object, MPISendMessage>> pendingSendMessages =
          pendingSendMessagesPerSource.get(source);

      RoutingParameters routingParameters = partialSendRoutingParameters(source, path);
      MPIMessage mpiMessage = new MPIMessage(source, type, MPIMessageDirection.OUT, this);
      int di = -1;
      if (routingParameters.getExternalRoutes().size() > 0) {
        di = routingParameters.getDestinationId();
      }
      // create a send message to keep track of the serialization
      // at the intial stage the sub-edge is 0
      MPISendMessage sendMessage = new MPISendMessage(source, mpiMessage, edge,
          di, path, routingParameters.getInternalRoutes(),
          routingParameters.getExternalRoutes());

      // now try to put this into pending
      boolean ret = pendingSendMessages.offer(
          new ImmutablePair<Object, MPISendMessage>(object, sendMessage));
      if (ret) {
        sendCount++;
      }
//      if (sendCount % 100 == 0) {
//        LOG.info(String.format("%d Partial Pending size: %d %d %d",
//            executor, pendingSendMessages.size(), sendCount,
//            pendingSendMessages.remainingCapacity()));
//      }
      return ret;
    } finally {
      lock.unlock();
    }
  }

  protected boolean sendMessage(int source, Object message, int path) {
    lock.lock();
    try {
      ArrayBlockingQueue<Pair<Object, MPISendMessage>> pendingSendMessages =
          pendingSendMessagesPerSource.get(source);

      RoutingParameters routingParameters = sendRoutingParameters(source, MPIContext.DEFAULT_PATH);
      MPIMessage mpiMessage = new MPIMessage(source, type, MPIMessageDirection.OUT, this);

      int di = -1;
      if (routingParameters.getExternalRoutes().size() > 0) {
        di = routingParameters.getDestinationId();
      }
      MPISendMessage sendMessage = new MPISendMessage(source, mpiMessage, edge,
          di, path, routingParameters.getInternalRoutes(),
          routingParameters.getExternalRoutes());

      // now try to put this into pending
      boolean ret = pendingSendMessages.offer(
          new ImmutablePair<Object, MPISendMessage>(message, sendMessage));
      if (ret) {
        sendCountFull++;
      }
//      if (sendCountFull % 100 == 0) {
//        LOG.info(String.format("%d Full Pending size: %d %d %d",
//            executor, pendingSendMessages.size(), sendCountFull,
//            pendingSendMessages.remainingCapacity()));
//      }
      return ret;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void progress() {
    lock.lock();
    try {
      int internalSends = 0;
      int externalSends = 0;
      int receives = 0;
      int internalSendsPending = 0;
      int externalSendsPending = 0;
      int receivesPending = 0;
      for (Map.Entry<Integer, ArrayBlockingQueue<Pair<Object, MPISendMessage>>> e
          : pendingSendMessagesPerSource.entrySet()) {

        Queue<Pair<Object, MPISendMessage>> pendingSendMessages = e.getValue();
        boolean canProgress = true;
        while (pendingSendMessages.size() > 0 && canProgress) {
          // take out pending messages
          Pair<Object, MPISendMessage> pair = pendingSendMessages.peek();
          MPISendMessage mpiMessage = pair.getValue();
          Object messageObject = pair.getKey();
          if (mpiMessage.serializedState() == MPISendMessage.SendState.INIT) {
            // send it internally
            for (Integer i : mpiMessage.getInternalSends()) {
              internalSendsPending++;
              // okay now we need to check weather this is the last place
              boolean receiveAccepted = receiveSendInternally(mpiMessage.getSource(), i,
                  mpiMessage.getPath(), messageObject);
              if (!receiveAccepted) {
                canProgress = false;
                break;
              }
              internalSends++;
            }
            if (canProgress) {
              mpiMessage.setSendState(MPISendMessage.SendState.SENT_INTERNALLY);
            }
          }

          if (canProgress) {
            // we don't have an external executor to send this message
            if (mpiMessage.getExternalSends().size() == 0) {
              pendingSendMessages.poll();
              continue;
            }

            // at this point lets build the message
            MPISendMessage message = (MPISendMessage)
                messageSerializer.build(pair.getKey(), mpiMessage);

            // okay we build the message, send it
            if (message.serializedState() == MPISendMessage.SendState.SERIALIZED) {
              List<Integer> exRoutes = new ArrayList<>(mpiMessage.getExternalSends());
              int startOfExternalRouts = mpiMessage.getAcceptedExternalSends();
              int noOfExternalSends = startOfExternalRouts;
              for (int i = startOfExternalRouts; i < exRoutes.size(); i++) {
                externalSendsPending++;
                boolean sendAccepted = sendMessageToTarget(message.getMPIMessage(),
                    exRoutes.get(i));
                // if no longer accepts stop
                if (!sendAccepted) {
                  canProgress = false;
                  break;
                } else {
                  mpiMessage.incrementAcceptedExternalSends();
                  noOfExternalSends++;
                  externalSends++;
                }
              }

              if (noOfExternalSends == exRoutes.size()) {
                // we are done
                mpiMessage.setSendState(MPISendMessage.SendState.FINISHED);
                pendingSendMessages.poll();
              }
            } else {
              break;
            }
          }
        }
      }

      for (Map.Entry<Integer, Queue<Pair<Object, MPIMessage>>> e
          : pendingReceiveMessagesPerSource.entrySet()) {
        Queue<Pair<Object, MPIMessage>> pendingReceiveMessages = e.getValue();

        while (pendingReceiveMessages.size() > 0) {
          Pair<Object, MPIMessage> pair = pendingReceiveMessages.peek();
          MPIMessage.ReceivedState state = pair.getRight().getReceivedState();
          MPIMessage currentMessage = pair.getRight();
          Object object = pair.getLeft();
          if (state == MPIMessage.ReceivedState.INIT) {
            currentMessage.incrementRefCount();
            currentMessage.setReceivedState(MPIMessage.ReceivedState.DOWN);
            if (!passMessageDownstream(object, currentMessage)) {
              break;
            }
            currentMessage.setReceivedState(MPIMessage.ReceivedState.RECEIVE);
            if (!receiveMessage(currentMessage, object)) {
              break;
            }
            receives++;
            currentMessage.release();
            pendingReceiveMessages.poll();
          } else if (state == MPIMessage.ReceivedState.DOWN) {
            currentMessage.setReceivedState(MPIMessage.ReceivedState.DOWN);
            if (!passMessageDownstream(object, currentMessage)) {
              break;
            }
            currentMessage.setReceivedState(MPIMessage.ReceivedState.RECEIVE);
            if (!receiveMessage(currentMessage, object)) {
              break;
            }
            receives++;
            currentMessage.release();
            pendingReceiveMessages.poll();
          } else if (state == MPIMessage.ReceivedState.RECEIVE) {
            currentMessage.setReceivedState(MPIMessage.ReceivedState.RECEIVE);
            if (!receiveMessage(currentMessage, object)) {
              break;
            }
            receives++;
            currentMessage.release();
            pendingReceiveMessages.poll();
          }
        }
      }

//      if (internalSendsPending > 0 || externalSendsPending > 0
//          || pendingReceiveMessages.size() > 0) {
//        if (internalSends == 0 && externalSends == 0 && receives == 0) {
//          LOG.info(String.format("%d No progress has been made %d %d %d",
//            executor, internalSendsPending, externalSendsPending, pendingReceiveMessages.size()));
//        }
//      }
    } finally {
      lock.unlock();
    }
  }

  private boolean sendMessageToTarget(MPIMessage msgObj1, int i) {
    msgObj1.incrementRefCount(1);
    int e = instancePlan.getExecutorForChannel(i);
    return channel.sendMessage(e, msgObj1, this);
  }

  @Override
  public void release(MPIMessage message) {
    if (message.doneProcessing()) {
      int originatingId = message.getOriginatingId();
      releaseTheBuffers(originatingId, message);
    }
  }

  @Override
  public void onSendComplete(int id, int messageStream, MPIMessage message) {
    // ok we don't have anything else to do
    message.release();
  }

  @Override
  public void close() {
  }

  protected void releaseTheBuffers(int id, MPIMessage message) {
    lock.lock();
    try {
      if (MPIMessageDirection.IN == message.getMessageDirection()) {
        Queue<MPIBuffer> list = receiveBuffers.get(id);
        for (MPIBuffer buffer : message.getBuffers()) {
          // we need to reset the buffer so it can be used again
          buffer.getByteBuffer().clear();
          list.offer(buffer);
        }
      } else if (MPIMessageDirection.OUT == message.getMessageDirection()) {
        Queue<MPIBuffer> queue = sendBuffers;
        for (MPIBuffer buffer : message.getBuffers()) {
          // we need to reset the buffer so it can be used again
          buffer.getByteBuffer().clear();
          queue.offer(buffer);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private int receiveCount = 0;

  @Override
  public void onReceiveComplete(int id, int e, MPIBuffer buffer) {
    lock.lock();
    try {
      // we need to try to build the message here, we may need many more messages to complete
      MPIMessage currentMessage = currentMessages.get(id);
      if (currentMessage == null) {
        currentMessage = new MPIMessage(id, type, MPIMessageDirection.IN, this);
        currentMessages.put(id, currentMessage);
      }

      Object object = messageDeSerializer.build(buffer, currentMessage, e);
      // if the message is complete, send it further down and call the receiver
      if (currentMessage.isComplete()) {
        Queue<Pair<Object, MPIMessage>> pendingReceiveMessages =
            pendingReceiveMessagesPerSource.get(id);

        receiveCount++;
        if (receiveCount % 100 == 0) {
          LOG.log(Level.INFO, String.format("%d received message %d %d %d",
              executor, id, receiveCount, pendingReceiveMessages.size()));
        }
        currentMessages.remove(id);

        currentMessage.setReceivedState(MPIMessage.ReceivedState.INIT);
        if (pendingReceiveMessages.size() > 0) {
          if (!pendingReceiveMessages.offer(new ImmutablePair<>(object, currentMessage))) {
            throw new RuntimeException(executor + " We should have enough space: "
                + pendingReceiveMessages.size());
          }
        } else {
          // we need to increment the ref count here before someone send it to another place
          // other wise they may free it before we do
          currentMessage.incrementRefCount();
          currentMessage.setReceivedState(MPIMessage.ReceivedState.DOWN);
          // we may need to pass this down to others
          if (!passMessageDownstream(object, currentMessage)) {
            if (!pendingReceiveMessages.offer(new ImmutablePair<>(object, currentMessage))) {
              throw new RuntimeException(executor + " We should have enough space: "
                  + pendingReceiveMessages.size());
            }
            return;
          }
          // we received a message, we need to determine weather we need to
          // forward to another node and process
          // check weather this is a message for partial or final receiver
          currentMessage.setReceivedState(MPIMessage.ReceivedState.RECEIVE);
          if (!receiveMessage(currentMessage, object)) {
            if (!pendingReceiveMessages.offer(new ImmutablePair<>(object, currentMessage))) {
              throw new RuntimeException(executor + " We should have enough space: "
                  + pendingReceiveMessages.size());
            }
            return;
          }
          // okay we built this message, lets remove it from the map
          // okay lets try to free the buffers of this message
          currentMessage.release();
        }
      } else {
        LOG.info("On receive complete message NOT built");
      }
    } finally {
      lock.unlock();
    }
  }
}
