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
package edu.iu.dsc.tws.comms.dfw;

import java.util.Set;

import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;

public class PartitionContext {
  private int groupLowWaterMark = 1;

  private int groupHighWaterMark = 1;

  private Set<Integer> sources;

  private Set<Integer> destinations;

  private int edge;

  private MessageReceiver finalRcvr;
  private MessageReceiver partialRcvr;

  private DataFlowPartition.PartitionStratergy partitionStratergy;
  private MessageType dataType;
  private MessageType keyType;

  public PartitionContext(Set<Integer> sources, Set<Integer> destinations, int edges,
                          MessageReceiver finalRcvr, MessageReceiver partialRcvr,
                          DataFlowPartition.PartitionStratergy partitionStratergy,
                          MessageType dataType, MessageType keyType) {
    this.sources = sources;
    this.destinations = destinations;
    this.finalRcvr = finalRcvr;
    this.partialRcvr = partialRcvr;
    this.partitionStratergy = partitionStratergy;
    this.dataType = dataType;
    this.keyType = keyType;
  }

  public int getGroupLowWaterMark() {
    return groupLowWaterMark;
  }

  public void setGroupLowWaterMark(int groupLowWaterMark) {
    this.groupLowWaterMark = groupLowWaterMark;
  }

  public int getGroupHighWaterMark() {
    return groupHighWaterMark;
  }

  public void setGroupHighWaterMark(int groupHighWaterMark) {
    this.groupHighWaterMark = groupHighWaterMark;
  }

  public Set<Integer> getSources() {
    return sources;
  }

  public Set<Integer> getDestinations() {
    return destinations;
  }

  public int getEdge() {
    return edge;
  }

  public MessageReceiver getFinalRcvr() {
    return finalRcvr;
  }

  public MessageReceiver getPartialRcvr() {
    return partialRcvr;
  }

  public DataFlowPartition.PartitionStratergy getPartitionStratergy() {
    return partitionStratergy;
  }

  public MessageType getDataType() {
    return dataType;
  }

  public MessageType getKeyType() {
    return keyType;
  }
}
