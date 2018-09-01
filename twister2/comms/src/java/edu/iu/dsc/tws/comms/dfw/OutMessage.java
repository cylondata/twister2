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

import edu.iu.dsc.tws.comms.dfw.io.SerializeState;

/**
 * Keep track of a message while it is transisitioning through the send phases
 */
public class OutMessage {
  // keep track of the serialized bytes in case we don't
  // have enough space in the send buffers
  protected SerializeState serializationState;

  //number of bytes copied to the network buffers so far
  private int byteCopied = 0;

  private int writtenHeaderSize = 0;

  private ChannelMessage ref;

  private boolean complete = false;

  private int source;

  private int edge;

  private int path;

  private int target;

  private Set<Integer> internalSends;

  private Set<Integer> externalSends;

  private int acceptedExternalSends = 0;
  private int acceptedInternalSends = 0;

  private int flags;

  private boolean outCountUpdated = false;

  public enum SendState {
    INIT,
    SENT_INTERNALLY,
    HEADER_BUILT,
    BODY_BUILT,
    PARTIALLY_SERIALIZED,
    SERIALIZED,
    FINISHED,
  }

  private SendState sendState = SendState.INIT;


  public OutMessage(int src, ChannelMessage message, int edge, int path, int target, int flags,
                    Set<Integer> intSends, Set<Integer> extSends) {
    this.ref = message;
    this.source = src;
    this.edge = edge;
    this.path = path;
    this.target = target;
    this.internalSends = intSends;
    this.externalSends = extSends;
    this.flags = flags;
  }

  public SendState serializedState() {
    return sendState;
  }

  public int getByteCopied() {
    return byteCopied;
  }

  public void setByteCopied(int byteCopied) {
    this.byteCopied = byteCopied;
  }

  public void setSendState(SendState sendState) {
    this.sendState = sendState;
  }

  public int getWrittenHeaderSize() {
    return writtenHeaderSize;
  }

  public void setWrittenHeaderSize(int writtenHeaderSize) {
    this.writtenHeaderSize = writtenHeaderSize;
  }

  public ChannelMessage getChannelMessage() {
    return ref;
  }

  public void setSerializationState(SerializeState serializationState) {
    this.serializationState = serializationState;
  }

  public SerializeState getSerializationState() {
    return serializationState;
  }

  public boolean isComplete() {
    return complete;
  }

  public void setComplete(boolean complete) {
    this.complete = complete;
  }

  public int getSource() {
    return source;
  }

  public int getEdge() {
    return edge;
  }

  public int getPath() {
    return path;
  }

  public int getTarget() {
    return target;
  }

  public Set<Integer> getInternalSends() {
    return internalSends;
  }

  public Set<Integer> getExternalSends() {
    return externalSends;
  }

  public int getAcceptedExternalSends() {
    return acceptedExternalSends;
  }

  public int incrementAcceptedExternalSends() {
    return ++acceptedExternalSends;
  }

  public int getAcceptedInternalSends() {
    return acceptedInternalSends;
  }

  public void incrementAcceptedInternalSends() {
    ++acceptedInternalSends;
  }

  public int getFlags() {
    return flags;
  }

  public void setOutCountUpdated(boolean outCountUpdated) {
    this.outCountUpdated = outCountUpdated;
  }

  public boolean isOutCountUpdated() {
    return outCountUpdated;
  }
}
