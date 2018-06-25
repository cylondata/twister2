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
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.logging.Logger;

public class ByteArrayOutputStream extends OutputStream {
  private static final Logger LOG = Logger.getLogger(ByteArrayOutputStream.class.getName());
  /**
   * The buffer where data is stored.
   */
  protected byte[] buf;

  /**
   * The number of valid bytes in the buffer.
   */
  protected int count;

  public ByteArrayOutputStream() {
    this(32);
  }


  public ByteArrayOutputStream(int size) {
    if (size < 0) {
      throw new IllegalArgumentException("Negative initial size: "
          + size);
    }
    buf = new byte[size];
  }


  private void ensureCapacity(int minCapacity) {
    // overflow-conscious code
    if (minCapacity - buf.length > 0) {
      grow(minCapacity);
    }
  }

  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  private void grow(int minCapacity) {
    // overflow-conscious code
    int oldCapacity = buf.length;
    int newCapacity = oldCapacity << 1;
    if (newCapacity - minCapacity < 0) {
      newCapacity = minCapacity;
    }
    if (newCapacity - MAX_ARRAY_SIZE > 0) {
      newCapacity = hugeCapacity(minCapacity);
    }
    buf = Arrays.copyOf(buf, newCapacity);
  }

  private static int hugeCapacity(int minCapacity) {
    if (minCapacity < 0) { // overflow
      throw new OutOfMemoryError();
    }
    return (minCapacity > MAX_ARRAY_SIZE) ? Integer.MAX_VALUE : MAX_ARRAY_SIZE;
  }

  public synchronized void write(int b) {
    ensureCapacity(count + 1);
    buf[count] = (byte) b;
    count += 1;
  }

  public synchronized void write(byte[] b, int off, int len) {
    if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
      throw new IndexOutOfBoundsException();
    }
    ensureCapacity(count + len);
    System.arraycopy(b, off, buf, count, len);
    count += len;
  }

  public synchronized void writeTo(OutputStream out) throws IOException {
    out.write(buf, 0, count);
  }

  public synchronized void reset() {
    count = 0;
  }

  public synchronized byte toByteArray()[] {
    return Arrays.copyOf(buf, count);
  }


  public synchronized int size() {
    return count;
  }

  public synchronized String toString() {
    return new String(buf, 0, count);
  }

  public synchronized String toString(String charsetName)
      throws UnsupportedEncodingException {
    return new String(buf, 0, count, charsetName);
  }

  /**
   * Deprecated method
   * @deprecated deprecated use
   * @param hibyte
   * @return
   */
  @Deprecated
  public synchronized String toString(int hibyte) {
    return new String(buf, hibyte, 0, count);
  }

  public void close() throws IOException {
  }
}
