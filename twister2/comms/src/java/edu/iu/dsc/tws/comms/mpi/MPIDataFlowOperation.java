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

import java.nio.ByteBuffer;
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
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageDeSerializer;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageSerializer;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.MPIMessageDeSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MPIMessageSerializer;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public abstract class MPIDataFlowOperation implements DataFlowOperation,
    MPIMessageListener, MPIMessageReleaseCallback {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowOperation.class.getName());

  public static final int MAX_ATTEMPTS = 1000;
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
  private int sendCountPartial = 0;
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

  protected Map<Integer, Queue<MPIMessage>> pendingReceiveDeSerializations;
  /**
   * Non grouped current messages
   */
  private Map<Integer, MPIMessage> currentMessages = new HashMap<>();

  protected KryoSerializer kryoSerializer;

  protected boolean debug;

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
    this.debug = false;
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
    this.pendingReceiveDeSerializations = new HashMap<>();

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
    messageSerializer.init(config);
    messageDeSerializer.init(config);
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
  protected abstract boolean receiveSendInternally(int source, int t, int path,
                                                   int flags, Object message);

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
  public boolean send(int source, Object message, int flags, int path) {
    return sendMessage(source, message, path, flags);
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int path) {
    return sendMessagePartial(source, message, path, flags);
  }

  /**
   * Setup the receives and send sendBuffers
   */
  protected void setupCommunication() {
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
  public boolean send(int source, Object message, int flags) {
    return sendMessage(source, message, MPIContext.DEFAULT_PATH, flags);
  }

  public boolean sendMessagePartial(int source, Object object, int path,
                                       int flags, MessageType t) {
    lock.lock();
    try {
      // for partial sends we use minus value to find the correct queue
      ArrayBlockingQueue<Pair<Object, MPISendMessage>> pendingSendMessages =
          pendingSendMessagesPerSource.get(source * -1 - 1);

      RoutingParameters routingParameters = partialSendRoutingParameters(source, path);
      MPIMessage mpiMessage = new MPIMessage(source, t, MPIMessageDirection.OUT, this);
      int di = -1;
      if (routingParameters.getExternalRoutes().size() > 0) {
        di = routingParameters.getDestinationId();
      }
      // create a send message to keep track of the serialization
      // at the intial stage the sub-edge is 0
      MPISendMessage sendMessage = new MPISendMessage(source, mpiMessage, edge,
          di, path, flags, routingParameters.getInternalRoutes(),
          routingParameters.getExternalRoutes());

      // now try to put this into pending
      boolean ret = pendingSendMessages.offer(
          new ImmutablePair<Object, MPISendMessage>(object, sendMessage));
      return ret;
    } finally {
      lock.unlock();
    }
  }

  protected boolean sendMessagePartial(int source, Object object, int path, int flags) {
    return sendMessagePartial(source, object, path, flags, type);
  }

  protected boolean sendMessage(int source, Object message, int path, int flags) {
    lock.lock();
    try {
      ArrayBlockingQueue<Pair<Object, MPISendMessage>> pendingSendMessages =
          pendingSendMessagesPerSource.get(source);

      RoutingParameters routingParameters = sendRoutingParameters(source, path);
      MPIMessage mpiMessage = new MPIMessage(source, type, MPIMessageDirection.OUT, this);

      int di = -1;
      if (routingParameters.getExternalRoutes().size() > 0) {
        di = routingParameters.getDestinationId();
      }
      MPISendMessage sendMessage = new MPISendMessage(source, mpiMessage, edge,
          di, path, flags, routingParameters.getInternalRoutes(),
          routingParameters.getExternalRoutes());

      // now try to put this into pending
      boolean ret = pendingSendMessages.offer(
          new ImmutablePair<Object, MPISendMessage>(message, sendMessage));
      return ret;
    } finally {
      lock.unlock();
    }
  }

  private int sendMessageToTargetAttempts = 0;
  private Map<Integer, Integer> sendMessageInternalAttempts = new HashMap<>();
  private Map<Integer, Integer> receiveMessageAttempts = new HashMap<>();

  private int updateAttemptMap(Map<Integer, Integer> map, int id, int count) {
    int attempt = 0;
    if (map.containsKey(id)) {
      attempt = map.get(id);
    }
    attempt += count;
    if (attempt < 0) {
      attempt = 0;
    }
    map.put(id, attempt);
    return attempt;
  }

  @Override
  public void progress() {
    lock.lock();
    try {
      for (Map.Entry<Integer, ArrayBlockingQueue<Pair<Object, MPISendMessage>>> e
          : pendingSendMessagesPerSource.entrySet()) {

        Queue<Pair<Object, MPISendMessage>> pendingSendMessages = e.getValue();
        boolean canProgress = true;
        while (pendingSendMessages.size() > 0 && canProgress) {
          // take out pending messages
          Pair<Object, MPISendMessage> pair = pendingSendMessages.peek();
          MPISendMessage mpiSendMessage = pair.getValue();
          Object messageObject = pair.getKey();
          if (mpiSendMessage.serializedState() == MPISendMessage.SendState.INIT) {
            // send it internally
            for (Integer i : mpiSendMessage.getInternalSends()) {
              // okay now we need to check weather this is the last place
              boolean receiveAccepted = receiveSendInternally(mpiSendMessage.getSource(), i,
                  mpiSendMessage.getPath(), mpiSendMessage.getFlags(), messageObject);
              if (!receiveAccepted) {
                canProgress = false;
                int attempt = updateAttemptMap(sendMessageInternalAttempts, i, 1);
                if (debug && attempt > MAX_ATTEMPTS) {
                  LOG.info(String.format("%d Send message internal attempts %d",
                      executor, attempt));
                }
                break;
              } else {
                updateAttemptMap(sendMessageInternalAttempts, i, -1);
              }
            }
            if (canProgress) {
              mpiSendMessage.setSendState(MPISendMessage.SendState.SENT_INTERNALLY);
            }
          }

          if (canProgress) {
            // we don't have an external executor to send this message
            if (mpiSendMessage.getExternalSends().size() == 0) {
              pendingSendMessages.poll();
              continue;
            }

            // at this point lets build the message
            MPISendMessage message = (MPISendMessage)
                messageSerializer.build(pair.getKey(), mpiSendMessage);

            // okay we build the message, send it
            if (message.serializedState() == MPISendMessage.SendState.SERIALIZED) {
              List<Integer> exRoutes = new ArrayList<>(mpiSendMessage.getExternalSends());
              int startOfExternalRouts = mpiSendMessage.getAcceptedExternalSends();
              int noOfExternalSends = startOfExternalRouts;
              for (int i = startOfExternalRouts; i < exRoutes.size(); i++) {
                boolean sendAccepted = sendMessageToTarget(message.getMPIMessage(),
                    exRoutes.get(i));
                // if no longer accepts stop
                if (!sendAccepted) {
                  canProgress = false;
                  sendMessageToTargetAttempts++;
                  if (sendMessageToTargetAttempts > MAX_ATTEMPTS) {
                    LOG.info(String.format("%d Send message target attempts %d",
                        executor, sendMessageToTargetAttempts));
                  }
                  break;
                } else {
                  if (sendMessageToTargetAttempts > 0) {
                    sendMessageToTargetAttempts--;
                  }
                  mpiSendMessage.incrementAcceptedExternalSends();
                  noOfExternalSends++;
                }
              }

              if (noOfExternalSends == exRoutes.size()) {
                // we are done
                mpiSendMessage.setSendState(MPISendMessage.SendState.FINISHED);
                pendingSendMessages.poll();
              }
            } else {
              break;
            }
          }
        }
      }

      for (Map.Entry<Integer, Queue<MPIMessage>> it : pendingReceiveDeSerializations.entrySet()) {
        MPIMessage currentMessage = it.getValue().poll();
        if (currentMessage == null) {
          continue;
        }

        Object object = messageDeSerializer.build(currentMessage,
            currentMessage.getHeader().getEdge());
        // if the message is complete, send it further down and call the receiver
        int id = currentMessage.getOriginatingId();
        Queue<Pair<Object, MPIMessage>> pendingReceiveMessages =
            pendingReceiveMessagesPerSource.get(id);
//        receiveCount++;
//        if (receiveCount % 1 == 0) {
//          LOG.info(String.format("%d received message %d %d %d",
//              executor, id, receiveCount, pendingReceiveMessages.size()));
//        }
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
            continue;
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
            int attempt = updateAttemptMap(receiveMessageAttempts, id, 1);
            if (debug && attempt >  MAX_ATTEMPTS) {
              LOG.info(String.format("%d Send message internal attempts %d",
                  executor, attempt));
            }
            continue;
          } else {
            updateAttemptMap(receiveMessageAttempts, id, -1);
          }
          // okay lets try to free the buffers of this message
          currentMessage.release();
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
          }

          if (state == MPIMessage.ReceivedState.DOWN || state == MPIMessage.ReceivedState.INIT) {
            currentMessage.setReceivedState(MPIMessage.ReceivedState.DOWN);
            if (!passMessageDownstream(object, currentMessage)) {
              break;
            }
            currentMessage.setReceivedState(MPIMessage.ReceivedState.RECEIVE);
            if (!receiveMessage(currentMessage, object)) {
              int attempt = updateAttemptMap(receiveMessageAttempts, 0, 1);
              if (debug && attempt > MAX_ATTEMPTS) {
                LOG.info(String.format("%d on message attempts %d",
                    executor, attempt));
              }
              break;
            } else {
              updateAttemptMap(receiveMessageAttempts, 0, -1);
            }
            currentMessage.release();
            pendingReceiveMessages.poll();
          } else if (state == MPIMessage.ReceivedState.RECEIVE) {
            currentMessage.setReceivedState(MPIMessage.ReceivedState.RECEIVE);
            if (!receiveMessage(currentMessage, object)) {
              int attempt = updateAttemptMap(receiveMessageAttempts, 0, 1);
              if (debug && attempt > MAX_ATTEMPTS) {
                LOG.info(String.format("%d on message attempts %d",
                    executor, attempt));
              }
              break;
            } else {
              updateAttemptMap(receiveMessageAttempts, 0, -1);
            }
            currentMessage.release();
            pendingReceiveMessages.poll();
          }
        }
      }
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
//      LOG.info(String.format("%d received message from %d", executor, id));
      MPIMessage currentMessage = currentMessages.get(id);
      ByteBuffer byteBuffer = buffer.getByteBuffer();
      byteBuffer.position(buffer.getSize());
      byteBuffer.flip();
      if (currentMessage == null) {
        currentMessage = new MPIMessage(id, type, MPIMessageDirection.IN, this);
        currentMessages.put(id, currentMessage);
        MessageHeader header = messageDeSerializer.buildHeader(buffer, e);
//        LOG.info(String.format("%d header source %d length %d", executor,
//            header.getSourceId(), header.getLength()));
        //TODO: What if we check the dest id and if its the id of the current running thread
        //TODO: Write to the memory manager, Still need to take out the massages
        currentMessage.setHeader(header);
        currentMessage.setHeaderSize(16);
      }
      // lets rewind to 0
      currentMessage.addBuffer(buffer);
      currentMessage.build();

      if (currentMessage.isComplete()) {
        currentMessages.remove(id);
        Queue<MPIMessage> deserializeQueue = pendingReceiveDeSerializations.get(id);
        if (!deserializeQueue.offer(currentMessage)) {
          throw new RuntimeException(executor + " We should have enough space: "
              + deserializeQueue.size());
        }
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void finish() {
  }

  public TaskPlan getInstancePlan() {
    return instancePlan;
  }

  public Config getConfig() {
    return config;
  }

  public MessageType getType() {
    return type;
  }
}
