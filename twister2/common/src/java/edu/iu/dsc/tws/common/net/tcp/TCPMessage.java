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
package edu.iu.dsc.tws.common.net.tcp;

import java.nio.ByteBuffer;

/**
 * Encapsulates a message and its data
 */
public class TCPMessage {
  /**
   * The buffer holding the data
   */
  private final ByteBuffer byteBuffer;

  /**
   * The position in the buffer
   */
  private int position;

  /**
   * An identifier used to distinguish between different message streams
   */
  private final int edge;

  /**
   * Status of the message
   */
  private TCPStatus status;

  /**
   * Length of the message
   */
  private int length;

  public TCPMessage(ByteBuffer buffer, int e, int l) {
    this.byteBuffer = buffer;
    this.edge = e;
    this.position = 0;
    this.status = TCPStatus.INIT;
    this.length = l;
  }

  public TCPStatus testStatus() {
    return status;
  }

  void setPosition(int position) {
    this.position = position;
  }

  int getPosition() {
    return position;
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  int getEdge() {
    return edge;
  }

  void setComplete(boolean complete) {
    if (complete) {
      this.status = TCPStatus.COMPLETE;
    } else {
      this.status = TCPStatus.INIT;
    }
  }

  public boolean isComplete() {
    return TCPStatus.COMPLETE == status;
  }

  public int getLength() {
    return length;
  }
}
