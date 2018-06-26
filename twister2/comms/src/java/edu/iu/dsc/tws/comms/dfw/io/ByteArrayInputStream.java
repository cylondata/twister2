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
package edu.iu.dsc.tws.comms.dfw.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import edu.iu.dsc.tws.comms.dfw.DataBuffer;

/**
 * This is a specialized input stream targetted to reading a twister object message expanding
 * to multiple MPI buffers.
 */
public class ByteArrayInputStream extends InputStream {
  // the buffers which contains the message
  protected List<DataBuffer> bufs;

  // the current buffer index
  protected int currentBufferIndex = 0;

  private int length;

  private int copiedBytes = 0;

  public ByteArrayInputStream(List<DataBuffer> buffers, int len) {
    this.bufs = buffers;
    this.currentBufferIndex = 0;
    this.length = len;
  }

  public synchronized int read() {
    ByteBuffer byteBuffer = getReadBuffer();
    // we are at the end
    if (byteBuffer == null) {
      return -1;
    }
    // check to see if this buffer has this information
    if (byteBuffer.remaining() >= 1) {
      copiedBytes++;
      return byteBuffer.get();
    } else {
      throw new RuntimeException("Failed to read the next byte");
    }
  }

  public synchronized int read(byte[] b, int off, int len) {
    ByteBuffer byteBuffer = getReadBuffer();
    // we are at the end
    if (byteBuffer == null) {
      return -1;
    }
    int canCopy = length - copiedBytes;
    // check to see if this buffer has this information
    if (byteBuffer.remaining() >= 1) {
      // we can copy upto len or remaining
      int copiedLength = 0;
      if (byteBuffer.remaining() > len) {
        copiedLength = len > canCopy ? canCopy : len;
      } else {
        copiedLength = byteBuffer.remaining() > canCopy ? canCopy : byteBuffer.remaining();
      }
      byteBuffer.get(b, off, copiedLength);
      copiedBytes += copiedLength;
      // increment position
      return copiedLength;
    } else {
      throw new RuntimeException("Failed to read the next byte");
    }
  }

  private ByteBuffer getReadBuffer() {
    if (copiedBytes >= length) {
      return null;
    }

    ByteBuffer byteBuffer = bufs.get(currentBufferIndex).getByteBuffer();
    // this is the intial time we are reading
    int pos = byteBuffer.position();

    // now check if we need to go to the next buffer
    if (pos >= byteBuffer.limit() - 1) {
      // if we are at the end we need to move to next
      currentBufferIndex++;
      byteBuffer = bufs.get(currentBufferIndex).getByteBuffer();
      byteBuffer.rewind();
      //we are at the end so return null
      if (currentBufferIndex >= bufs.size()) {
        return null;
      }
    }
    return byteBuffer;
  }

  public synchronized long skip(long n) {
    if (n < 0) {
      return 0;
    }

    int skipped = 0;
    for (int i = currentBufferIndex; i < bufs.size(); i++) {
      ByteBuffer b = bufs.get(i).getByteBuffer();
      int avail;
      long needSkip = n - skipped;
      int bufPos = b.position();

      avail = b.remaining() - bufPos;
      int canCopy = length - (copiedBytes + skipped);
      // now check how much we need to move here
      if (needSkip >= avail) {
        avail = canCopy > avail ? avail : canCopy;
        // we go to the end
        b.position(bufPos + avail);
        currentBufferIndex++;
        skipped += avail;
        copiedBytes += skipped;
      } else {
        needSkip = canCopy > needSkip ? needSkip : canCopy;
        b.position((int) (bufPos + needSkip));
        skipped += needSkip;
        copiedBytes += skipped;
      }

      if (skipped >= n) {
        break;
      }
    }
    return skipped;
  }

  public synchronized int available() {
    return length;
  }

  public boolean markSupported() {
    return false;
  }

  public void mark(int readAheadLimit) {
  }

  public synchronized void reset() {
    copiedBytes = 0;
    currentBufferIndex = 0;
  }

  public void close() throws IOException {
  }
}
