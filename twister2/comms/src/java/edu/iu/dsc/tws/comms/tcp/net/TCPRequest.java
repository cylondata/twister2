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
package edu.iu.dsc.tws.comms.tcp.net;

import java.nio.ByteBuffer;

public class TCPRequest {
  private final ByteBuffer byteBuffer;

  private int position;

  private final int edge;

  private TCPStatus status;

  private boolean complete;

  private int length;

  TCPRequest(ByteBuffer buffer, int e) {
    this.byteBuffer = buffer;
    this.edge = e;
    this.position = 0;
    this.status = new TCPStatus();
    this.complete = false;
  }

  public TCPRequest(ByteBuffer buffer, int e, int l) {
    this.byteBuffer = buffer;
    this.edge = e;
    this.position = 0;
    this.status = new TCPStatus();
    this.complete = false;
    this.length = l;
  }

  public TCPStatus testStatus() {
    if (complete) {
      return status;
    }
    return null;
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
    this.complete = complete;
  }

  public boolean isComplete() {
    return complete;
  }

  public int getLength() {
    return length;
  }
}
