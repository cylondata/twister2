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
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.table.channel.ChannelReceiveCallback;
import edu.iu.dsc.tws.comms.table.channel.ChannelSendCallback;
import edu.iu.dsc.tws.comms.table.channel.TRequest;

public class SimpleAllToAll implements ChannelReceiveCallback, ChannelSendCallback {
  private static final Logger LOG = Logger.getLogger(SimpleAllToAll.class.getName());

  private enum AllToAllSendStatus {
    SENDING,
    FINISH_SENT,
    ALL_FINISHED
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

  private List<AllToAllSends> sends;

  private Set<Integer> finishedSources;

  private Set<Integer> finishedTargets;

  public boolean insert(ByteBuffer buf, int length,  int target) {
    return false;
  }

  public boolean insert(ByteBuffer buf, int length, ByteBuffer header,
                        int headerLength,  int target) {
    return false;
  }

  public boolean isComplete() {
    return false;
  }

  public void finish(int source) {

  }

  public void close() {

  }

  @Override
  public void receivedData(int receiveId, ByteBuffer buffer, int length) {

  }

  @Override
  public void receivedHeader(int receiveId, int finished, int[] header, int headerLength) {

  }

  @Override
  public void sendComplete(TRequest request) {

  }

  @Override
  public void sendFinishComplete(TRequest request) {

  }
}
