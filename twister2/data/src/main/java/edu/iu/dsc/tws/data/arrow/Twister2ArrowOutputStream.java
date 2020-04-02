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
package edu.iu.dsc.tws.data.arrow;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.logging.Logger;

public class Twister2ArrowOutputStream implements WritableByteChannel, Serializable {

  private static final Logger LOG = Logger.getLogger(Twister2ArrowOutputStream.class.getName());

  private boolean isOpen;
  private byte[] tempBuffer;
  private long bytesSoFar;

  private transient FileOutputStream fileOutputStream;

  public Twister2ArrowOutputStream(FileOutputStream fileoutputStream) {
    this.isOpen = true;
    this.tempBuffer = new byte[1024 * 1024];
    this.bytesSoFar = 0;
    this.fileOutputStream = fileoutputStream;
  }

  @Override
  public int write(ByteBuffer byteBuffer) throws IOException {
    int remaining = byteBuffer.remaining();
    int soFar = 0;
    while (soFar < remaining) {
      int toPush = Math.min(remaining - soFar, this.tempBuffer.length);
      byteBuffer.get(this.tempBuffer, 0, toPush);
      this.fileOutputStream.write(this.tempBuffer, 0, toPush);
      soFar += toPush;
    }
    this.bytesSoFar += remaining;
    return remaining;
  }

  @Override
  public boolean isOpen() {
    return this.isOpen;
  }

  @Override
  public void close() throws IOException {
    this.fileOutputStream.close();
    this.isOpen = false;
  }
}
