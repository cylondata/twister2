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
package edu.iu.dsc.tws.comms.table.channel;

import java.nio.ByteBuffer;

public final class TRequest {
  protected ByteBuffer buffer;
  protected int length;
  protected int target;
  protected int[] header;
  protected int headerLength;

  public TRequest(int tgt, ByteBuffer buf, int len) {
    target = tgt;
    buffer = buf;
    length = len;
  }

  public TRequest(int tgt, ByteBuffer data, int len, int[] head, int hLength) {
    target = tgt;
    buffer = data;
    length = len;
    // we are copying the header
    header = head;
    headerLength = hLength;
  }

  public TRequest(int tgt) {
    target = tgt;
  }

  public int getLength() {
    return length;
  }

  public int getTarget() {
    return target;
  }

  public int[] getHeader() {
    return header;
  }

  public int getHeaderLength() {
    return headerLength;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }
}
