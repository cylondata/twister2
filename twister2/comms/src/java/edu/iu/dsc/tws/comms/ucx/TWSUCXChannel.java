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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
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

public class TWSUCXChannel implements TWSChannel {

  private static final Logger LOG = Logger.getLogger(TWSUCXChannel.class.getName());

  private final List<Closeable> closeables = new ArrayList<>();
  private final Map<Integer, UcpEndpoint> endpoints = new HashMap<>();

  private UcpWorker ucpWorker;

  private AtomicLong pendingSendRequests = new AtomicLong();

  private int workerId;

  private int tagWIdOffset = 100000;

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
    ucpWorker.newListener(new UcpListenerParams().setSockAddr(
        new InetSocketAddress("0.0.0.0", iWorkerController.getWorkerInfo().getPort())
    ));
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
    }
  }

  @Override
  public boolean sendMessage(int id, ChannelMessage message, ChannelListener callback) {
    AtomicInteger buffersLeft = new AtomicInteger(message.getBuffers().size());
    for (DataBuffer buffer : message.getBuffers()) {
      //write total limit of buffer
      buffer.getByteBuffer().putInt(0, buffer.getSize());

      buffer.getByteBuffer().limit(buffer.getSize());
      buffer.getByteBuffer().position(0);
      int tag = this.workerId * tagWIdOffset + message.getHeader().getEdge();
      LOG.info(String.format("SENDING to %d[%d] : %s, TAG[%d]", id, message.getHeader().getEdge(),
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


  class ReceiveProgress {

    private int group;
    private int id;
    private int edge;
    private ChannelListener callback;
    private Queue<DataBuffer> receiveBuffers;

    ReceiveProgress(int group, int id, int edge,
                    ChannelListener callback, Queue<DataBuffer> receiveBuffers) {
      this.group = group;
      this.id = id;
      this.edge = edge;
      this.callback = callback;
      this.receiveBuffers = receiveBuffers;
    }

    public void progress() {
      while (!receiveBuffers.isEmpty()) {
        final DataBuffer recvBuffer = receiveBuffers.poll();
        int tag = id * tagWIdOffset + edge;
        LOG.info(String.format("EXPECTING from TAG: %d, Buffer : %s", tag,
            recvBuffer.getByteBuffer()));
        ucpWorker.recvTaggedNonBlocking(
            recvBuffer.getByteBuffer(),
            tag,
            0xffff,
            new UcxCallback() {
              @Override
              public void onSuccess(UcpRequest request) {
                LOG.info(String.format("Recv Buff from %d[%d] : %s, TAG[%d], Size : %d",
                    id, edge, recvBuffer.getByteBuffer(), tag,
                    recvBuffer.getByteBuffer().getInt(0)));
                LOG.info(String.format("Sizes %d:%d", request.getRecvSize(),
                    recvBuffer.getByteBuffer().getInt(0)));
                recvBuffer.setSize(recvBuffer.getByteBuffer().getInt(0));
                callback.onReceiveComplete(id, edge, recvBuffer);
              }

              @Override
              public void onError(int ucsStatus, String errorMsg) {
                // This is a catastrophic failure
                String failedMsg = "Failed to receive from " + id + " with status "
                    + ucsStatus + ". Error : " + errorMsg;
                LOG.severe(failedMsg);
                throw new Twister2RuntimeException(failedMsg);
              }
            }
        );
      }
    }
  }

  private List<ReceiveProgress> receiveProgresses = new ArrayList<>();

  @Override
  public boolean receiveMessage(int group, int id, int edge, ChannelListener callback,
                                Queue<DataBuffer> receiveBuffers) {
    ReceiveProgress receiveProgress = new ReceiveProgress(group, id,
        edge, callback, receiveBuffers);
    receiveProgress.progress();
    this.receiveProgresses.add(receiveProgress);
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

  private int count = 0;

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
    this.endpoints.get(wId).close();
  }
}
