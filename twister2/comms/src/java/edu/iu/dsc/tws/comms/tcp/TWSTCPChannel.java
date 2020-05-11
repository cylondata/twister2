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
package edu.iu.dsc.tws.comms.tcp;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.channel.ChannelListener;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.comms.messaging.ChannelMessage;
import edu.iu.dsc.tws.api.comms.packing.DataBuffer;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.common.net.NetworkInfo;
import edu.iu.dsc.tws.common.net.tcp.TCPChannel;
import edu.iu.dsc.tws.common.net.tcp.TCPContext;
import edu.iu.dsc.tws.common.net.tcp.TCPMessage;
import edu.iu.dsc.tws.common.net.tcp.TCPStatus;
import edu.iu.dsc.tws.common.util.IterativeLinkedList;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;

public class TWSTCPChannel implements TWSChannel {
  private static final Logger LOG = Logger.getLogger(TWSTCPChannel.class.getName());

  private int executor;

  private int sendCount = 0;

  private int pendingSendCount = 0;

  private TCPChannel channel;
  private IWorkerController workerController;
  private long maxConnEstTime;

  @SuppressWarnings("VisibilityModifier")
  private class Request {
    TCPMessage request;
    DataBuffer buffer;

    Request(TCPMessage request, DataBuffer buffer) {
      this.request = request;
      this.buffer = buffer;
    }
  }

  @SuppressWarnings("VisibilityModifier")
  private class TCPReceiveRequests {
    IterativeLinkedList<Request> pendingRequests;
    int rank;
    int edge;
    ChannelListener callback;
    Queue<DataBuffer> availableBuffers;

    TCPReceiveRequests(int rank, int e,
                       ChannelListener callback, Queue<DataBuffer> buffers) {
      this.rank = rank;
      this.edge = e;
      this.callback = callback;
      this.availableBuffers = buffers;
      this.pendingRequests = new IterativeLinkedList<>();
    }
  }

  @SuppressWarnings("VisibilityModifier")
  private class TCPSendRequests {
    IterativeLinkedList<Request> pendingSends;
    int rank;
    int edge;
    ChannelMessage message;
    ChannelListener callback;

    TCPSendRequests(int rank, int e,
                    ChannelMessage message, ChannelListener callback) {
      this.rank = rank;
      this.edge = e;
      this.message = message;
      pendingSends = new IterativeLinkedList<>();
      this.callback = callback;
    }
  }

  private int completedReceives = 0;

  /**
   * Pending sends waiting to be posted
   */
  private ArrayBlockingQueue<TCPSendRequests> pendingSends;

  /**
   * These are the places where we expect to receive messages
   */
  private List<TCPReceiveRequests> registeredReceives;

  /**
   * The grouped receives
   */
  private Int2ObjectArrayMap<List<TCPReceiveRequests>> groupedRegisteredReceives;

  /**
   * Wait for completion sends
   */
  private IterativeLinkedList<TCPSendRequests> waitForCompletionSends;

  /**
   * Holds requests that are pending for close
   */
  private List<Pair<Integer, Integer>> pendingCloseRequests = new ArrayList<>();

  /**
   * Create the TCP channel
   * @param config configuration
   * @param wController controller
   */
  public TWSTCPChannel(Config config,
                       IWorkerController wController) {
    int index = wController.getWorkerInfo().getWorkerID();
    int workerPort = wController.getWorkerInfo().getPort();
    String localIp = wController.getWorkerInfo().getWorkerIP();

    workerController = wController;
    maxConnEstTime = TCPContext.maxConnEstTime(config);

    channel = createChannel(config, localIp, workerPort, index);
    // now lets start listening before sending the ports to master,
    channel.startListening();

    // wait for everyone to start the job master
    try {
      workerController.waitOnBarrier();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      throw new Twister2RuntimeException(timeoutException);
    }

    // now talk to a central server and get the information about the worker
    // this is a synchronization step
    List<JobMasterAPI.WorkerInfo> wInfo = workerController.getJoinedWorkers();

    // lets start the client connections now
    List<NetworkInfo> nInfos = new ArrayList<>();
    for (JobMasterAPI.WorkerInfo w : wInfo) {
      NetworkInfo networkInfo = new NetworkInfo(w.getWorkerID());
      networkInfo.addProperty(TCPContext.NETWORK_PORT, w.getPort());
      networkInfo.addProperty(TCPContext.NETWORK_HOSTNAME, w.getWorkerIP());
      nInfos.add(networkInfo);
    }

    // start the connections
    channel.startConnections(nInfos);
    // now lets wait for connections to be established
    channel.waitForConnections(maxConnEstTime);

    int pendingSize = CommunicationContext.networkChannelPendingSize(config);
    this.pendingSends = new ArrayBlockingQueue<>(pendingSize);
    this.registeredReceives = new ArrayList<>(1024);
    this.groupedRegisteredReceives = new Int2ObjectArrayMap<>();
    this.waitForCompletionSends = new IterativeLinkedList<>();
    this.executor = wController.getWorkerInfo().getWorkerID();
  }

  @Override
  public void reInit(List<JobMasterAPI.WorkerInfo> restartedWorkers) {

    // close previous connections
    for (JobMasterAPI.WorkerInfo wInfo: restartedWorkers) {
      channel.closeConnection(wInfo.getWorkerID());
    }

    // wait for everyone to start the job master
    try {
      workerController.waitOnBarrier();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      throw new Twister2RuntimeException(timeoutException);
    }

    // lets start the client connections now
    List<NetworkInfo> nInfos = new ArrayList<>();
    for (JobMasterAPI.WorkerInfo w : restartedWorkers) {
      NetworkInfo networkInfo = new NetworkInfo(w.getWorkerID());
      networkInfo.addProperty(TCPContext.NETWORK_PORT, w.getPort());
      networkInfo.addProperty(TCPContext.NETWORK_HOSTNAME, w.getWorkerIP());
      nInfos.add(networkInfo);
    }

    // start the connections
    channel.startConnections(nInfos);
    // now lets wait for connections to be established
    channel.waitForConnections(maxConnEstTime);
  }
  /**
   * Start the TCP servers here
   *
   * @param cfg the configuration
   * @param workerId worker id
   */
  private static TCPChannel createChannel(Config cfg, String workerIP, int workerPort,
                                          int workerId) {
    NetworkInfo netInfo = new NetworkInfo(workerId);
    netInfo.addProperty(TCPContext.NETWORK_HOSTNAME, workerIP);
    netInfo.addProperty(TCPContext.NETWORK_PORT, workerPort);
    return new TCPChannel(cfg, netInfo);
  }

  /**
   * Send messages to the particular id
   *
   * @param id id to be used for sending messages
   * @param message the message
   * @return true if the message is accepted to be sent
   */
  public boolean sendMessage(int id, ChannelMessage message, ChannelListener callback) {
    boolean offer = pendingSends.offer(
        new TCPSendRequests(id, message.getHeader().getEdge(), message, callback));
    if (offer) {
      pendingSendCount++;
    }
    return offer;
  }

  /**
   * Register our interest to receive messages from particular rank using a stream
   */
  public boolean receiveMessage(int group, int rank, int stream,
                                ChannelListener callback, Queue<DataBuffer> receiveBuffers) {
    TCPReceiveRequests requests = new TCPReceiveRequests(rank, stream, callback,
        receiveBuffers);
    registeredReceives.add(requests);
    List<TCPReceiveRequests> list;
    if (groupedRegisteredReceives.containsKey(group)) {
      list = groupedRegisteredReceives.get(group);
    } else {
      list = new ArrayList<>();
      groupedRegisteredReceives.put(group, list);
    }
    list.add(requests);
    return true;
  }

  @Override
  public void close() {
    // we will call the comm stop
    while (!this.pendingCloseRequests.isEmpty()
        || !this.pendingSends.isEmpty() || !this.waitForCompletionSends.isEmpty()) {
      this.progress();
    }
    channel.stop();
  }

  @Override
  public boolean isComplete() {
    // nothing to do here
    if (!this.pendingCloseRequests.isEmpty()
        || !this.pendingSends.isEmpty() || !this.waitForCompletionSends.isEmpty()) {
      return false;
    }
    return true;
  }

  /**
   * Send a message to the given rank.
   *
   * @param requests the message
   */
  private void postMessage(TCPSendRequests requests) {
    ChannelMessage message = requests.message;
    for (int i = 0; i < message.getNormalBuffers().size(); i++) {
      sendCount++;
      DataBuffer buffer = message.getNormalBuffers().get(i);
      TCPMessage request = channel.iSend(buffer.getByteBuffer(), buffer.getSize(),
          requests.rank, message.getHeader().getEdge());
      // register to the loop to make communicationProgress on the send
      requests.pendingSends.add(new Request(request, buffer));
    }
  }

  private void postReceive(TCPReceiveRequests requests) {
    DataBuffer byteBuffer = requests.availableBuffers.poll();
    if (byteBuffer != null) {
      // post the receive
      TCPMessage request = postReceive(requests.rank, requests.edge, byteBuffer);
      requests.pendingRequests.add(new Request(request, byteBuffer));
    }
  }

  /**
   * Post the receive request to MPI
   *
   * @param rank MPI rank
   * @param stream the stream as a tag
   * @param byteBuffer the buffer
   * @return the request
   */
  private TCPMessage postReceive(int rank, int stream, DataBuffer byteBuffer) {
    return channel.iRecv(byteBuffer.getByteBuffer(), byteBuffer.getCapacity(), rank, stream);
  }

  private void internalProgressReceives(List<TCPReceiveRequests> requests) {
    for (int i = 0; i < requests.size(); i++) {
      TCPReceiveRequests receiveRequests = requests.get(i);

      // okay we have more buffers to be posted
      if (receiveRequests.availableBuffers.size() > 0) {
        postReceive(receiveRequests);
      }

      IterativeLinkedList.ILLIterator requestIterator = receiveRequests.pendingRequests.iterator();
      while (requestIterator.hasNext()) {
        Request r = (Request) requestIterator.next();
        if (r == null || r.request == null) {
          continue;
        }
        TCPStatus status = r.request.testStatus();
        if (status == TCPStatus.COMPLETE) {
          // lets call the callback about the receive complete
          r.buffer.setSize(r.buffer.getByteBuffer().limit());

          //We do not have any buffers to receive messages so we need to free a buffer
          receiveRequests.callback.onReceiveComplete(
              receiveRequests.rank, receiveRequests.edge, r.buffer);
          requestIterator.remove();
        }
      }
    }
    // handle the pending close requests
    handlePendingCloseRequests();
  }

  @Override
  public void progress() {
    progressSends();

    internalProgressReceives(registeredReceives);

    channel.progress();
  }

  @Override
  public void progressSends() {
    // we should rate limit here
    while (pendingSends.size() > 0) {
      // post the message
      TCPSendRequests sendRequests = pendingSends.poll();
      // post the send
      postMessage(sendRequests);
      waitForCompletionSends.add(sendRequests);
    }

    IterativeLinkedList.ILLIterator sendRequestsIterator
        = waitForCompletionSends.iterator();
    while (sendRequestsIterator.hasNext()) {
      TCPSendRequests sendRequests = (TCPSendRequests) sendRequestsIterator.next();
      IterativeLinkedList.ILLIterator requestIterator
          = sendRequests.pendingSends.iterator();
      while (requestIterator.hasNext()) {
        Request r = (Request) requestIterator.next();
        TCPStatus status = r.request.testStatus();
        // this request has finished
        if (status == TCPStatus.COMPLETE) {
          requestIterator.remove();
        }
      }

      // if the message if fully sent, lets call the callback
      // ideally we should be able to call for each finish of the buffer
      if (sendRequests.pendingSends.size() == 0) {
        sendRequests.callback.onSendComplete(sendRequests.rank,
            sendRequests.edge, sendRequests.message);
        sendRequestsIterator.remove();
      }
    }
  }

  @Override
  public void progressReceives(int group) {
    internalProgressReceives(groupedRegisteredReceives.get(group));
  }

  /**
   * Close all the pending requests
   */
  private void handlePendingCloseRequests() {
    while (pendingCloseRequests.size() > 0) {
      Pair<Integer, Integer> closeRequest = pendingCloseRequests.remove(0);

      // clear up the receive requests
      Iterator<TCPReceiveRequests> itr = registeredReceives.iterator();
      while (itr.hasNext()) {
        TCPReceiveRequests receiveRequests = itr.next();
        if (receiveRequests.edge == closeRequest.getRight()
            && receiveRequests.rank == closeRequest.getLeft()) {
          IterativeLinkedList.ILLIterator pendItr
              = receiveRequests.pendingRequests.iterator();
          while (pendItr.hasNext()) {
            Request r = (Request) pendItr.next();
            r.request.isComplete();
            pendItr.remove();
          }
          itr.remove();
        }
      }
      // lets not handle any send requests as they will complete eventually
    }
  }

  @Override
  public ByteBuffer createBuffer(int capacity) {
    return ByteBuffer.allocate(capacity);
  }

  /**
   * Close a worker id with edge
   *
   * @param workerId worker id
   * @param e edge
   */
  public void releaseBuffers(int workerId, int e) {
    pendingCloseRequests.add(new ImmutablePair<>(workerId, e));
  }
}
