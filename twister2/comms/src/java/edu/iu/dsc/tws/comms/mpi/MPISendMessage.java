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

import java.util.Set;

import edu.iu.dsc.tws.comms.mpi.io.SerializeState;

/**
 * Keep track of a MPI message while it is transisitioning through the send phases
 */
public class MPISendMessage {
  // keep track of the serialized bytes in case we don't
  // have enough space in the send buffers
  protected SerializeState serializationState;

  //number of bytes copied to the network buffers so far
  private int byteCopied = 0;

  private int writtenHeaderSize = 0;

  private MPIMessage ref;

  private boolean complete = false;

  private int source;

  private int edge;

  private int destintationIdentifier;

  private int path;

  private Set<Integer> internalSends;

  private Set<Integer> externalSends;

  private int acceptedExternalSends = 0;
  private int acceptedInternalSends = 0;

  private int flags;

  public enum SendState {
    INIT,
    SENT_INTERNALLY,
    HEADER_BUILT,
    BODY_BUILT,
    SERIALIZED,
    FINISHED,
  }

  private SendState sendState = SendState.INIT;


  public MPISendMessage(int src, MPIMessage message, int e, int di, int p, int f,
                        Set<Integer> intSends, Set<Integer> extSends) {
    this.ref = message;
    this.source = src;
    this.edge = e;
    this.destintationIdentifier = di;
    this.path = p;
    this.internalSends = intSends;
    this.externalSends = extSends;
    this.flags = f;
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

  public MPIMessage getMPIMessage() {
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

  public int getDestintationIdentifier() {
    return destintationIdentifier;
  }

  public int getPath() {
    return path;
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

  public int incrementAcceptedInternalSends() {
    return ++acceptedInternalSends;
  }

  public int getFlags() {
    return flags;
  }
}
