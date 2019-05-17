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
package edu.iu.dsc.tws.executor.core.checkpoint;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import edu.iu.dsc.tws.comms.api.DataPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.ObjectPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.StringPacker;
import edu.iu.dsc.tws.task.api.checkpoint.Snapshot;

public class SnapshotImpl implements Snapshot {

  private Long version = -1L;
  private Map<String, Object> values = new HashMap<>();
  private Map<String, DataPacker> packers = new HashMap<>();

  @Override
  public void setPacker(String key, DataPacker dataPacker) {
    this.packers.put(key, dataPacker);
  }

  @Override
  public void setState(String key, Object value) {
    this.values.put(key, value);
  }

  @Override
  public Long getVersion() {
    return this.version;
  }

  public void setVersion(Long version) {
    this.version = version;
  }

  public byte[] pack() {
    Map<String, byte[]> packedValues = new HashMap<>();
    Map<String, byte[]> packedKeys = new HashMap<>();
    int totalLength = 0;
    for (String key : values.keySet()) {
      DataPacker packer = packers.getOrDefault(key, ObjectPacker.getInstance());
      byte[] dataBytes = packer.packToByteArray(values.get(key));
      byte[] keyBytes = StringPacker.getInstance().packToByteArray(key);
      packedValues.put(key, dataBytes);
      packedKeys.put(key, keyBytes);
      totalLength += dataBytes.length + keyBytes.length + (2 * Integer.BYTES);
    }
    ByteBuffer byteBuffer = ByteBuffer.allocate(totalLength);
    for (String key : values.keySet()) {
      byte[] keyBytes = packedKeys.get(key);
      byteBuffer.putInt(keyBytes.length);
      byteBuffer.put(keyBytes);

      byte[] dataBytes = packedValues.get(key);
      byteBuffer.putInt(dataBytes.length);
      byteBuffer.put(dataBytes);
    }
    return byteBuffer.array();
  }

  public void unpack(byte[] bytes) {
    ByteBuffer wrapped = ByteBuffer.wrap(bytes);
    while (wrapped.position() < bytes.length) {
      int keyLength = wrapped.getInt();
      byte[] keyBytes = new byte[keyLength];
      wrapped.get(keyBytes);

      int valueLength = wrapped.getInt();
      byte[] valueBytes = new byte[valueLength];
      wrapped.get(valueBytes);

      String key = StringPacker.getInstance().unpackFromByteArray(keyBytes);
      Object value = this.packers.getOrDefault(key,
          ObjectPacker.getInstance()).unpackFromByteArray(valueBytes);
      this.values.put(key, value);
    }
  }
}
