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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.TCPChannel;
import edu.iu.dsc.tws.common.net.tcp.TCPMessage;
import edu.iu.dsc.tws.common.net.tcp.TCPStatus;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.dfw.ChannelListener;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;

public class TWSTCPChannel implements TWSChannel {
  private static final Logger LOG = Logger.getLogger(TWSTCPChannel.class.getName());

  // a lock object to be used
  private Lock lock = new ReentrantLock();

  private int executor;

  private int sendCount = 0;

  private int pendingSendCount = 0;

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
    List<Request> pendingRequests;
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
      this.pendingRequests = new ArrayList<>();
    }
  }

  @SuppressWarnings("VisibilityModifier")
  private class TCPSendRequests {
    List<Request> pendingSends;
    int rank;
    int edge;
    ChannelMessage message;
    ChannelListener callback;

    TCPSendRequests(int rank, int e,
                    ChannelMessage message, ChannelListener callback) {
      this.rank = rank;
      this.edge = e;
      this.message = message;
      pendingSends = new ArrayList<>();
      this.callback = callback;
    }
  }

  /**
   * Pending sends waiting to be posted
   */
  private ArrayBlockingQueue<TCPSendRequests> pendingSends;

  /**
   * These are the places where we expect to receive messages
   */
  private List<TCPReceiveRequests> registeredReceives;

  /**
   * Wait for completion sends
   */
  private List<TCPSendRequests> waitForCompletionSends;

  private TCPChannel comm;

  public TWSTCPChannel(Config config, int exec, TCPChannel net) {
    this.pendingSends = new ArrayBlockingQueue<TCPSendRequests>(1024);
    this.registeredReceives = Collections.synchronizedList(new ArrayList<>(1024));
    this.waitForCompletionSends = Collections.synchronizedList(new ArrayList<>(1024));
    this.executor = exec;
    this.comm = net;
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
   * @param rank
   * @param stream
   * @param callback
   * @return
   */
  public boolean receiveMessage(int rank, int stream,
                                ChannelListener callback, Queue<DataBuffer> receiveBuffers) {
    return registeredReceives.add(new TCPReceiveRequests(rank, stream, callback,
        receiveBuffers));
  }

  @Override
  public void close() {
    // we will call the comm stop
    comm.stop();
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
      TCPMessage request = comm.iSend(buffer.getByteBuffer(), buffer.getSize(),
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
   * @param rank MPI rank
   * @param stream the stream as a tag
   * @param byteBuffer the buffer
   * @return the request
   */
  private TCPMessage postReceive(int rank, int stream, DataBuffer byteBuffer) {
    return comm.iRecv(byteBuffer.getByteBuffer(), byteBuffer.getCapacity(), rank, stream);
  }

  private int completedReceives = 0;
  /**
   * Progress the communications that are pending
   */
  public void progress() {
    // we should rate limit here
    while (pendingSends.size() > 0) {
      // post the message
      TCPSendRequests sendRequests = pendingSends.poll();
      // post the send
      postMessage(sendRequests);
      waitForCompletionSends.add(sendRequests);
    }

    for (int i = 0; i < registeredReceives.size(); i++) {
      TCPReceiveRequests receiveRequests = registeredReceives.get(i);
      // okay we have more buffers to be posted
      if (receiveRequests.availableBuffers.size() > 0) {
        postReceive(receiveRequests);
      }
    }

    Iterator<TCPSendRequests> sendRequestsIterator = waitForCompletionSends.iterator();
    while (sendRequestsIterator.hasNext()) {
      TCPSendRequests sendRequests = sendRequestsIterator.next();
      Iterator<Request> requestIterator = sendRequests.pendingSends.iterator();
      while (requestIterator.hasNext()) {
        Request r = requestIterator.next();
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


    for (int i = 0; i < registeredReceives.size(); i++) {
      TCPReceiveRequests receiveRequests = registeredReceives.get(i);
      Iterator<Request> requestIterator = receiveRequests.pendingRequests.iterator();
      while (requestIterator.hasNext()) {
        Request r = requestIterator.next();
        if (r == null || r.request == null) {
          continue;
        }
        TCPStatus status = r.request.testStatus();
        if (status == TCPStatus.COMPLETE) {
          // lets call the callback about the receive complete
          r.buffer.setSize(r.buffer.getByteBuffer().limit());
          receiveRequests.callback.onReceiveComplete(
              receiveRequests.rank, receiveRequests.edge, r.buffer);
          requestIterator.remove();
          if (receiveRequests.pendingRequests.size() == 0
              && receiveRequests.availableBuffers.size() == 0) {
            //We do not have any buffers to receive messages so we need to free a buffer
            receiveRequests.callback.freeReceiveBuffers(receiveRequests.rank,
                receiveRequests.edge);
          }
        }
      }
    }

    comm.progress();
  }

  @Override
  public ByteBuffer createBuffer(int capacity) {
    return ByteBuffer.allocate(capacity);
  }
}
