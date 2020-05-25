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
package edu.iu.dsc.tws.comms.table;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.comms.table.channel.ChannelReceiveCallback;
import edu.iu.dsc.tws.comms.table.channel.ChannelSendCallback;
import edu.iu.dsc.tws.comms.table.channel.MPIChannel;
import edu.iu.dsc.tws.comms.table.channel.TRequest;

public class SimpleAllToAll implements ChannelReceiveCallback, ChannelSendCallback {
  private static final Logger LOG = Logger.getLogger(SimpleAllToAll.class.getName());

  private enum AllToAllSendStatus {
    SENDING,
    FINISH_SENT,
    FINISHED
  }

  private class AllToAllSends {
    private int target;
    private Queue<TRequest> requestQueue;
    private Queue<TRequest> pendingQueue;
    private int messageSizes;
    private AllToAllSendStatus sendStatus = AllToAllSendStatus.SENDING;
  }

  private List<Integer> sources;
  private List<Integer> targets;
  private List<AllToAllSends> sends = new ArrayList<>();
  private Set<Integer> finishedSources = new HashSet<>();
  private Set<Integer> finishedTargets = new HashSet<>();
  private MPIChannel channel;
  private int edgeId;
  private boolean finishFlag = false;
  private ReceiveCallback callback;

  public SimpleAllToAll(Config cfg, IWorkerController workerController,
                        List<Integer> sources, List<Integer> targets,
                        int edgeId, ReceiveCallback callback) {
    this.sources = sources;
    this.targets = targets;
    this.channel = new MPIChannel(cfg, workerController, edgeId, sources,
        targets, this, this);
    this.edgeId = edgeId;
    this.callback = callback;
  }

  public boolean insert(ByteBuffer buf, int length, int target) {
    return false;
  }

  public boolean insert(ByteBuffer buf, int length, int[] header,
                        int headerLength,  int target) {
    if (finishFlag) {
      throw new Twister2RuntimeException("Cannot insert after finishing");
    }

    if (headerLength > MPIChannel.TWISTERX_CHANNEL_USER_HEADER) {
      throw new Twister2RuntimeException("Cannot have a header length greater than "
          + MPIChannel.TWISTERX_CHANNEL_USER_HEADER);
    }

    AllToAllSends s = sends.get(target);
    TRequest request = new TRequest(target, buf, length, header, headerLength);
    s.requestQueue.offer(request);
    s.messageSizes += length;
    return false;
  }

  public boolean isComplete() {
    boolean allQueuesEmpty = true;
    for (AllToAllSends s : sends) {
      while (!s.requestQueue.isEmpty()) {
        if (s.sendStatus == AllToAllSendStatus.FINISH_SENT ||
            s.sendStatus == AllToAllSendStatus.FINISHED) {
          String msg = "We cannot have items to send after finish sent";
          LOG.log(Level.SEVERE, msg);
          throw new Twister2RuntimeException(msg);
        }

        TRequest request = s.requestQueue.peek();
        if (1 == channel.send(request)) {
          s.requestQueue.poll();
          s.pendingQueue.offer(request);
        }
      }

      if (s.pendingQueue.isEmpty()) {
        if (finishFlag) {
          if (s.sendStatus == AllToAllSendStatus.SENDING) {
            TRequest request = new TRequest(s.target);
            if (1 == channel.sendFin(request)) {
              s.sendStatus = AllToAllSendStatus.FINISH_SENT;
            }
          }
        }
      } else {
        allQueuesEmpty = false;
      }
    }
    channel.progressReceives();
    channel.progressSends();
    return allQueuesEmpty && finishedTargets.size() == targets.size()
        && finishedSources.size() == sources.size();
  }

  public void finish(int source) {
    finishFlag = true;
  }

  public void close() {
    sends.clear();

  }

  @Override
  public void receivedData(int receiveId, ByteBuffer buffer, int length) {
    callback.onReceive(receiveId, buffer, length);
  }

  @Override
  public void receivedHeader(int receiveId, int finished, int[] header, int headerLength) {
    if (finished == 1) {
      finishedSources.add(receiveId);
      callback.onReceiveHeader(receiveId, true, header, headerLength);
    } else {
      if (headerLength > 0) {
        callback.onReceiveHeader(receiveId, false, header, headerLength);
      } else {
        callback.onReceiveHeader(receiveId, false, null, 0);
      }
    }
  }

  @Override
  public void sendComplete(TRequest request) {
    AllToAllSends s = sends.get(request.getTarget());
    s.pendingQueue.poll();
    s.messageSizes -= request.getLength();
    callback.onSendComplete(request.getTarget(), request.getBuffer(), request.getLength());
  }

  @Override
  public void sendFinishComplete(TRequest request) {
    finishedTargets.add(request.getTarget());
    AllToAllSends s = sends.get(request.getTarget());
    s.sendStatus = AllToAllSendStatus.FINISHED;
  }
}
