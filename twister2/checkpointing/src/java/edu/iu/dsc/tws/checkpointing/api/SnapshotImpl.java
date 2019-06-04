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

package edu.iu.dsc.tws.checkpointing.api;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import edu.iu.dsc.tws.comms.api.DataPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.ObjectPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.StringPacker;

/**
 * This class implements {@link Snapshot} and will have some additional methods to
 * manipulate version and pack and unpack snapshot.User interfacing methods should always
 * accept {@link Snapshot} interface which doesn't contain those additional methods.
 */
public class SnapshotImpl implements Snapshot {

  private long version = 0L;
  private Map<String, Object> values = new HashMap<>();
  private Map<String, DataPacker> packers = new HashMap<>();

  private String prefix;

  public SnapshotImpl() {
/*
    this("");
*/
  }

/*
  public SnapshotImpl(String prefix) {
    this.prefix = prefix;
  }

  private String appendPrefix(String key) {
    return prefix + key;
  }
*/

  @Override
  public void setPacker(String key, DataPacker dataPacker) {
    this.packers.put(key, dataPacker);
/*
    this.packers.put(appendPrefix(key), dataPacker);
*/
  }

  @Override
  public void setValue(String key, Object value) {
    this.values.put(key, value);
/*
    this.values.put(appendPrefix(key), value);
*/
  }

  @Override
  public long getVersion() {
    return this.version;
  }

  @Override
  public boolean isCheckpointed(String key) {
    return values.containsKey(key);
  }

  public void setVersion(long version) {
    this.version = version;
  }

  @Override
  public Object getOrDefault(String key, Object defaultValue) {
    return this.values.getOrDefault(key, defaultValue);
  }

  @Override
  public Object get(String key) {
    return this.values.get(key);
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
    ByteBuffer byteBuffer = ByteBuffer.allocate(totalLength + Long.BYTES);
    byteBuffer.putLong(this.version);
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
    this.setVersion(wrapped.getLong());
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SnapshotImpl snapshot = (SnapshotImpl) o;
    return Objects.equals(version, snapshot.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version);
  }

  public SnapshotImpl copy() {
    SnapshotImpl copy = new SnapshotImpl();
    copy.packers = this.packers;
    copy.values = new HashMap<>(this.values);
    copy.version = this.version;

    this.values.clear();
    return copy;
  }
}
