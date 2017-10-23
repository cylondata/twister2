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

/**
 * Keep track of a MPI message while it is transisitioning through the send phases
 */
public class MPISendMessage {
  // keep track of the serialized bytes in case we don't
  // have enough space in the send buffers
  protected byte[] sendBytes;

  //number of bytes copied to the network buffers so far
  private int byteCopied = 0;

  private int writtenHeaderSize = 0;

  private MPIMessage ref;

  private boolean complete = false;

  private int source;

  public enum SerializedState {
    INIT,
    HEADER_BUILT,
    BODY,
    FINISHED
  }

  private SerializedState serializedState;

  public MPISendMessage(int src, MPIMessage message) {
    this.ref = message;
    this.source = src;
  }

  public SerializedState serializedState() {
    return serializedState;
  }

  public int getByteCopied() {
    return byteCopied;
  }

  public void setByteCopied(int byteCopied) {
    this.byteCopied = byteCopied;
  }

  public void setSerializedState(SerializedState serializedState) {
    this.serializedState = serializedState;
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

  public void setSendBytes(byte[] sendBytes) {
    this.sendBytes = sendBytes;
  }

  public byte[] getSendBytes() {
    return sendBytes;
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
}
