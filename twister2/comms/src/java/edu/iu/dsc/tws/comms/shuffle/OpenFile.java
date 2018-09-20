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
package edu.iu.dsc.tws.comms.shuffle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * Represent an open file containing the channel and the buffer
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class OpenFile {
  private FileChannel rwChannel;
  private ByteBuffer byteBuffer;
  private List<KeyValue> keyValues;
  private int totalBytes;

  public OpenFile(FileChannel rwChannel, ByteBuffer buffer) {
    this.rwChannel = rwChannel;
    this.byteBuffer = buffer;
  }

  public OpenFile(FileChannel rwChannel, ByteBuffer byteBuffer,
                  List<KeyValue> kValues, int total) {
    this.rwChannel = rwChannel;
    this.byteBuffer = byteBuffer;
    this.keyValues = kValues;
    this.totalBytes = total;
  }

  public FileChannel getRwChannel() {
    return rwChannel;
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  public void close() throws IOException {
    rwChannel.close();
  }

  public List<KeyValue> getKeyValues() {
    return keyValues;
  }

  public int getTotalBytes() {
    return totalBytes;
  }
}
