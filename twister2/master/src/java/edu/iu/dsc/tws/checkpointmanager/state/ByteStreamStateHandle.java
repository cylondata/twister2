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
package edu.iu.dsc.tws.checkpointmanager.state;

import java.io.IOException;

import edu.iu.dsc.tws.data.fs.FSDataInputStream;

public class ByteStreamStateHandle implements StreamStateHandle {

  private static final long serialVersionUID = -5280226231202517594L;

  private final byte[] data;

  private final String handleName;

  public ByteStreamStateHandle(String handleName, byte[] data) {
    this.handleName = handleName;
    this.data = data;
  }

  @Override
  public FSDataInputStream openInputStream() throws IOException {
    return new ByteStateHandleInputStream(data);
  }

  @Override
  public void discardState() throws Exception {

  }

  @Override
  public long getStateSize() {
    return data.length;
  }

  public byte[] getData() {
    return data;
  }

  public String getHandleName() {
    return handleName;
  }


  private static final class ByteStateHandleInputStream extends FSDataInputStream {

    private final byte[] data;
    private int index;

    protected ByteStateHandleInputStream(byte[] data) {
      this.data = data;
    }

    @Override
    public void seek(long desired) throws IOException {
      if (desired >= 0 && desired <= data.length) {
        index = (int) desired;
      } else {
        throw new IOException("position out of bounds");
      }
    }

    @Override
    public long getPos() throws IOException {
      return index;
    }

    @Override
    public int read() throws IOException {
      return index < data.length ? data[index++] & 0xFF : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      final int bytesLeft = data.length - index;
      if (bytesLeft > 0) {
        final int bytesToCopy = Math.min(len, bytesLeft);
        System.arraycopy(data, index, b, off, bytesToCopy);
        index += bytesToCopy;
        return bytesToCopy;
      } else {
        return -1;
      }
    }
  }
}
