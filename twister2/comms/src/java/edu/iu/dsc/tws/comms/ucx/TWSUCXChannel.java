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
package edu.iu.dsc.tws.comms.ucx;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerParams;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;

import edu.iu.dsc.tws.api.comms.channel.ChannelListener;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.comms.messaging.ChannelMessage;
import edu.iu.dsc.tws.api.comms.packing.DataBuffer;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * This class uses UCX framework underneath to connect to the twister2's network
 * of Worker processes. {@link TWSUCXChannel} leverages tags based communication of UCX
 * to virtually create multiple communication channels between the workers based on the edge.
 * The tag for each message is calculated as follows.
 * <p>
 * tag = sendingWorkerId * tagWIdOffset + edge
 * </p>
 *
 * @since 0.5.0
 */
public class TWSUCXChannel implements TWSChannel {

  private static final Logger LOG = Logger.getLogger(TWSUCXChannel.class.getName());

  private final List<Closeable> closeables = new ArrayList<>();
  private final Map<Integer, UcpEndpoint> endpoints = new HashMap<>();

  private UcpWorker ucpWorker;

  private AtomicLong pendingSendRequests = new AtomicLong();

  private int workerId;

  private int tagWIdOffset = 100000;

  private List<ReceiveProgress> receiveProgresses = new ArrayList<>();
  private Map<Integer, Map<Integer, Set<ReceiveProgress>>> groupReceives = new HashMap<>();

  public TWSUCXChannel(Config config,
                       IWorkerController workerController) {
    createUXCWorker(workerController);
    this.workerId = workerController.getWorkerInfo().getWorkerID();
  }

  private void createUXCWorker(IWorkerController iWorkerController) {
    UcpContext context = new UcpContext(new UcpParams().requestTagFeature()
        .setMtWorkersShared(true));
    this.ucpWorker = context.newWorker(new UcpWorkerParams().requestThreadSafety());

    // start listener
    UcpListener ucpListener = ucpWorker.newListener(new UcpListenerParams().setSockAddr(
        new InetSocketAddress(iWorkerController.getWorkerInfo().getWorkerIP(),
            iWorkerController.getWorkerInfo().getPort())
    ));
    this.closeables.add(ucpListener);
    this.closeables.add(context);
    this.closeables.add(ucpWorker);

    try {
      // wait till everyone add listeners
      iWorkerController.waitOnBarrier();
    } catch (TimeoutException e) {
      LOG.log(Level.SEVERE, "Failed to wait on barrier", e);
    }

    // create end points
    for (JobMasterAPI.WorkerInfo worker : iWorkerController.getJoinedWorkers()) {
      UcpEndpoint ucpEndpoint = ucpWorker.newEndpoint(new UcpEndpointParams().setSocketAddress(
          new InetSocketAddress(worker.getWorkerIP(), worker.getPort())
      ));
      this.endpoints.put(worker.getWorkerID(), ucpEndpoint);
      this.closeables.add(ucpEndpoint);
    }
  }

  @Override
  public boolean sendMessage(int id, ChannelMessage message, ChannelListener callback) {
    AtomicInteger buffersLeft = new AtomicInteger(message.getBuffers().size());
    for (DataBuffer buffer : message.getBuffers()) {
      buffer.getByteBuffer().limit(buffer.getSize());
      buffer.getByteBuffer().position(0);
      int tag = this.workerId * tagWIdOffset + message.getHeader().getEdge();
      LOG.log(Level.FINE, () ->
          String.format("SENDING to %d[%d] : %s, TAG[%d]", id, message.getHeader().getEdge(),
              buffer.getByteBuffer(), tag));
      this.endpoints.get(id).sendTaggedNonBlocking(
          buffer.getByteBuffer(),
          tag,
          new UcxCallback() {
            @Override
            public void onSuccess(UcpRequest request) {
              pendingSendRequests.decrementAndGet();
              if (buffersLeft.decrementAndGet() == 0) {
                callback.onSendComplete(id, message.getHeader().getEdge(), message);
              }
            }

            @Override
            public void onError(int ucsStatus, String errorMsg) {
              // This is a catastrophic failure
              LOG.severe("UCX send request failed to worker " + id
                  + " with status " + ucsStatus + ". Error : " + errorMsg);
              throw new Twister2RuntimeException("Send request to worker : " + id + " failed. "
                  + errorMsg);
            }
          }
      );
      this.pendingSendRequests.incrementAndGet();
    }
    return true;
  }


  class ReceiveProgress implements Closeable {

    private int group;
    private int id;
    private int edge;
    private ChannelListener callback;
    private Queue<DataBuffer> receiveBuffers;

    private AtomicLong requestIdCounter = new AtomicLong();

    private Map<Long, UcpRequest> requestsMap = new ConcurrentHashMap<>();

    private boolean closed = false;

    ReceiveProgress(int group, int id, int edge,
                    ChannelListener callback, Queue<DataBuffer> receiveBuffers) {
      this.group = group;
      this.id = id;
      this.edge = edge;
      this.callback = callback;
      this.receiveBuffers = receiveBuffers;
    }

    /**
     * This method will cancel all the requests posted and free the buffers
     */
    @Override
    public void close() {
      if (!this.closed) {
        this.closed = true;
        this.requestsMap.values().forEach(request -> {
          try {
            ucpWorker.cancelRequest(request);
          } catch (NullPointerException nex) {
            // ignored, already cancelled
          }
        });
      }
    }

    public void progress() {
      while (!receiveBuffers.isEmpty() && !closed) {
        final DataBuffer recvBuffer = receiveBuffers.poll();
        int tag = id * tagWIdOffset + edge;
        LOG.log(Level.FINE, () -> String.format("EXPECTING from TAG: %d, Buffer : %s", tag,
            recvBuffer.getByteBuffer()));
        final long requestId = requestIdCounter.incrementAndGet();
        UcpRequest ucpRequest = ucpWorker.recvTaggedNonBlocking(
            recvBuffer.getByteBuffer(),
            tag,
            0xffff,
            new UcxCallback() {
              @Override
              public void onSuccess(UcpRequest request) {
                LOG.log(Level.FINE, () ->
                    String.format("Recv Buff from %d[%d] : %s, TAG[%d], Size : %d",
                        id, edge, recvBuffer.getByteBuffer(), tag,
                        recvBuffer.getByteBuffer().getInt(0)));
                recvBuffer.setSize((int) request.getRecvSize());
                requestsMap.remove(requestId);
                callback.onReceiveComplete(id, edge, recvBuffer);
              }

              @Override
              public void onError(int ucsStatus, String errorMsg) {
                if (ucsStatus != -16) { // status -16(cancelled) is ignored
                  // This is a catastrophic failure
                  String failedMsg = "Failed to receive from " + id + " with status "
                      + ucsStatus + ". Error : " + errorMsg;
                  LOG.severe(failedMsg);
                  requestsMap.remove(requestId);
                  throw new Twister2RuntimeException(failedMsg);
                }
              }
            }
        );
        requestsMap.put(requestId, ucpRequest);
      }
    }
  }

  @Override
  public boolean receiveMessage(int group, int id, int edge, ChannelListener callback,
                                Queue<DataBuffer> receiveBuffers) {
    ReceiveProgress receiveProgress = new ReceiveProgress(group, id,
        edge, callback, receiveBuffers);
    receiveProgress.progress();
    this.receiveProgresses.add(receiveProgress);
    this.groupReceives.computeIfAbsent(id, wi -> new HashMap<>())
        .computeIfAbsent(edge, edgeId -> new HashSet<>()).add(receiveProgress);
    return true;
  }

  @Override
  public void progress() {
    for (ReceiveProgress receiveProgress : this.receiveProgresses) {
      receiveProgress.progress();
    }
    this.ucpWorker.progress();
  }

  @Override
  public void progressSends() {
    this.progress();
  }

  @Override
  public void progressReceives(int group) {
    this.progress();
  }

  @Override
  public boolean isComplete() {
    return pendingSendRequests.get() == 0;
  }

  @Override
  public ByteBuffer createBuffer(int capacity) {
    return ByteBuffer.allocateDirect(capacity);
  }

  @Override
  public void close() {
    for (Closeable closeable : this.closeables) {
      try {
        closeable.close();
      } catch (IOException e) {
        throw new Twister2RuntimeException("Failed to close UCX channel component : "
            + closeable, e);
      }
    }
  }

  @Override
  public void releaseBuffers(int wId, int e) {
    for (ReceiveProgress receiveProgress : this.groupReceives.getOrDefault(wId,
        Collections.emptyMap()).getOrDefault(e, Collections.emptySet())) {
      receiveProgress.close();
    }
  }
}
