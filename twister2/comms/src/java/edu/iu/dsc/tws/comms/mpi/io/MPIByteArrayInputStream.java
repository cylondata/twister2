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
package edu.iu.dsc.tws.comms.mpi.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import edu.iu.dsc.tws.comms.mpi.MPIBuffer;

/**
 * This is a specialized input stream targetted to reading a twister message expanding
 * to multiple MPI buffers.
 */
public class MPIByteArrayInputStream extends InputStream {
  protected List<MPIBuffer> bufs;

  protected int pos;

  protected int mark = 0;

  protected int currentBufferIndex = 0;

  protected int headerSize;

  protected boolean grouped;

  public MPIByteArrayInputStream(List<MPIBuffer> buffers, int headerSize, boolean group) {
    this.bufs = buffers;
    this.pos = 0;
    this.currentBufferIndex = 0;
    this.headerSize = headerSize;
    this.grouped = group;
  }

  public synchronized int read() {
    ByteBuffer byteBuffer = getReadBuffer();
    // we are at the end
    if (byteBuffer == null) {
      return -1;
    }
    // check to see if this buffer has this information
    if (byteBuffer.remaining() >= 1) {
      pos += 1;
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
    // check to see if this buffer has this information
    if (byteBuffer.remaining() >= 1) {
      // we can copy upto len or remaining
      int copiedLength = byteBuffer.remaining() > len ? len : byteBuffer.remaining();
      byteBuffer.get(b, off, copiedLength);
      // increment position
      pos += copiedLength;
      return copiedLength;
    } else {
      throw new RuntimeException("Failed to read the next byte");
    }
  }

  private ByteBuffer getReadBuffer() {
    ByteBuffer byteBuffer = bufs.get(currentBufferIndex).getByteBuffer();
    // this is the intial time we are reading
    if (currentBufferIndex == 0 && pos == 0 && byteBuffer.remaining() > headerSize) {
      // lets rewind the buffer so the position becomes 0
      byteBuffer.rewind();
      // now skip the header size
      byteBuffer.position(headerSize);
    } else {
      // we don't have enough data in the buffer to read the header
      throw new RuntimeException("The buffer doesn't contain data or complete header");
    }

    // now check if we need to go to the next buffer
    if (pos >= byteBuffer.limit() - 1) {
      // if we are at the end we need to move to next
      currentBufferIndex++;
      pos = 0;
      byteBuffer = bufs.get(currentBufferIndex).getByteBuffer();
      byteBuffer.rewind();
      //we are at the end so return -1
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
      int avail = 0;
      long needSkip = n - skipped;
      int bufPos = b.position();

      // we need to skip header
      if (i == 0) {
        if (bufPos < headerSize) {
          // lets go to the header
          b.position(headerSize);
          bufPos = headerSize;
        }
      }

      avail = b.remaining() - bufPos;
      // now check how much we need to move here
      if (needSkip >= avail) {
        // we go to the end
        b.position(bufPos + avail);
        currentBufferIndex++;
        pos = 0;
        skipped += avail;
      } else {
        b.position((int) (bufPos + needSkip));
        skipped += needSkip;
        pos = (int) (bufPos + needSkip);
      }
    }
    return skipped;
  }

  public synchronized int available() {
    int avail = 0;
    for (int i = currentBufferIndex; i < bufs.size(); i++) {
      ByteBuffer b = bufs.get(i).getByteBuffer();
      if (i == 0) {
        int position = b.position();
        if (position > headerSize) {
          avail += b.remaining() - position;
        } else {
          avail += b.remaining() - headerSize;
        }
      } else {
        avail += b.remaining();
      }
    }
    return avail;
  }


  public boolean markSupported() {
    return false;
  }


  public void mark(int readAheadLimit) {
    mark = pos;
  }

  public synchronized void reset() {
    pos = mark;
  }

  public void close() throws IOException {
  }
}
