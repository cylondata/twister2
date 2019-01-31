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

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.io.SerializeState;

/**
 * Keep track of a message while it is transitioning through the send phases
 */
public class OutMessage {
  public enum SendState {
    INIT,
    SENT_INTERNALLY,
    HEADER_BUILT,
    PARTIALLY_SERIALIZED,
    SERIALIZED,
  }

  /**
   * keep track of the serialized bytes in case we don't have enough space in the send buffers
   */
  private SerializeState serializationState;

  /**
   * Weather message is complete
   */
  private boolean complete = false;

  /**
   * Originating source
   */
  private int source;

  /**
   * Edge to be used
   */
  private int edge;

  /**
   * Path
   */
  private int path;

  /**
   * Target task
   */
  private int target;

  /**
   * The internal send ids
   */
  private Set<Integer> internalSends;

  /**
   * The external send ids
   */
  private Set<Integer> externalSends;

  /**
   * Message flags
   */
  private int flags;

  /**
   * Accepted internal sends of this message
   */
  private int acceptedInternalSends = 0;

  /**
   * Channel messages created for sending this message through network
   */
  private Queue<ChannelMessage> channelMessages = new LinkedBlockingQueue<>();

  /**
   * Keep track of the send state
   */
  private SendState sendState = SendState.INIT;

  /**
   * The data type of the message
   */
  private MessageType dataType;

  /**
   * Key type of the message
   */
  private MessageType keyType;

  /**
   * The release callback
   */
  private ChannelMessageReleaseCallback releaseCallback;

  public OutMessage(int src, int edge, int path, int target, int flags,
                    Set<Integer> intSends, Set<Integer> extSends,
                    MessageType dataType, MessageType keyType,
                    ChannelMessageReleaseCallback releaseCallback) {
    this.source = src;
    this.edge = edge;
    this.path = path;
    this.target = target;
    this.internalSends = intSends;
    this.externalSends = extSends;
    this.flags = flags;
    this.serializationState = new SerializeState();
    this.dataType = dataType;
    this.keyType = keyType;
    this.releaseCallback = releaseCallback;
  }

  public SendState serializedState() {
    return sendState;
  }

  public void setSendState(SendState sendState) {
    this.sendState = sendState;
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

  public int getAcceptedInternalSends() {
    return acceptedInternalSends;
  }

  public void incrementAcceptedInternalSends() {
    ++acceptedInternalSends;
  }

  public int getFlags() {
    return flags;
  }

  public Queue<ChannelMessage> getChannelMessages() {
    return channelMessages;
  }

  public MessageType getDataType() {
    return dataType;
  }

  public MessageType getKeyType() {
    return keyType;
  }

  public ChannelMessageReleaseCallback getReleaseCallback() {
    return releaseCallback;
  }
}
