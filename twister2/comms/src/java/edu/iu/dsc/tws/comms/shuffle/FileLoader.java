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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;

public final class FileLoader {
  private static final Logger LOG = Logger.getLogger(FileLoader.class.getName());

  private FileLoader() {
  }

  /**
   * Save the list of records to the file system
   * @param records records to be written
   * @param size total size of the records
   * @param outFileName out file name
   */
  public static void saveObjects(List<byte[]> records, List<Integer> sizes,
                                 long size, String outFileName) {
    try {
      FileChannel rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
      for (int i = 0; i < records.size(); i++) {
        byte[] r = records.get(i);
        os.putInt(sizes.get(i));
        os.put(r, 0, sizes.get(i));
      }
      rwChannel.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed write to disc", e);
      throw new RuntimeException(e);
    }
  }



  /**
   * Save the list of records to the file system
   * @param records records to be written
   * @param size total size of the records
   * @param outFileName out file name
   */
  public static int saveKeyValues(List<KeyValue> records, List<Integer> sizes,
                                   long size, String outFileName, MessageType keyType,
                                   KryoMemorySerializer serializer) {
    try {
      // first serialize keys
      int totalSize = 0;
      List<byte[]> byteKeys = new ArrayList<>();
      if (keyType == MessageType.OBJECT) {
        for (KeyValue record : records) {
          byte[] data = serializer.serialize(record.getKey());
          totalSize += data.length;
          byteKeys.add(data);
        }
      } else {
        for (KeyValue record : records) {
          if (keyType == MessageType.BYTE) {
            byte[] key = (byte[]) record.getKey();
            totalSize += key.length;
          } else if (keyType == MessageType.DOUBLE) {
            double[] d = (double[]) record.getKey();
            totalSize += d.length * 8;
          } else if (keyType == MessageType.INTEGER) {
            int[] d = (int[]) record.getKey();
            totalSize += d.length * 4;
          } else if (keyType == MessageType.LONG) {
            long[] d = (long[]) record.getKey();
            totalSize += d.length * 8;
          } else if (keyType == MessageType.CHAR) {
            char[] d = (char[]) record.getKey();
            totalSize += d.length;
          } else if (keyType == MessageType.SHORT) {
            short[] d = (short[]) record.getKey();
            totalSize += d.length * 2;
          }
        }
      }
      totalSize += size;

      FileChannel rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, totalSize);
      for (int i = 0; i < records.size(); i++) {
        KeyValue keyValue = records.get(i);
        byte[] r = (byte[]) keyValue.getValue();
        // serialize key with its length
        if (keyType == MessageType.OBJECT) {
          byte[] src = byteKeys.get(i);
          os.putInt(src.length);
          os.put(src);
        } else if (keyType == MessageType.BYTE) {
          byte[] key = (byte[]) keyValue.getKey();
          os.putInt(key.length);
          os.put(key);
        } else if (keyType == MessageType.DOUBLE) {
          double[] kd = (double[]) keyValue.getKey();
          os.putInt(kd.length);
          for (double d : kd) {
            os.putDouble(d);
          }
        } else if (keyType == MessageType.INTEGER) {
          int[] kd = (int[]) keyValue.getKey();
          os.putInt(kd.length);
          for (int d : kd) {
            os.putInt(d);
          }
        } else if (keyType == MessageType.LONG) {
          long[] kd = (long[]) keyValue.getKey();
          os.putInt(kd.length);
          for (long d : kd) {
            os.putLong(d);
          }
        } else if (keyType == MessageType.CHAR) {
          char[] kd = (char[]) keyValue.getKey();
          os.putInt(kd.length);
          for (char d : kd) {
            os.putChar(d);
          }
        } else if (keyType == MessageType.SHORT) {
          short[] kd = (short[]) keyValue.getKey();
          os.putInt(kd.length);
          for (short d : kd) {
            os.putShort(d);
          }
        }
        os.putInt(sizes.get(i));
        os.put(r, 0, sizes.get(i));
      }
      rwChannel.close();
      return totalSize;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed write to disc", e);
      throw new RuntimeException(e);
    }
  }

  public static List<KeyValue> readFile(String fileName, MessageType keyType,
                                        MessageType dataType, KryoMemorySerializer deserializer) {
    String outFileName = Paths.get(fileName).toString();
    FileChannel rwChannel;
    try {
      rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_ONLY, 0, rwChannel.size());

      List<KeyValue> keyValues = new ArrayList<>();
      // lets read the key values
      int totalRead = 0;
      while (totalRead < rwChannel.size()) {
        Object key;
        Object value;

        int keySize = os.getInt();
        key = deserialize(keyType, deserializer, os, keySize);

        int dataSize = os.getInt();
        value = deserialize(dataType, deserializer, os, dataSize);
        keyValues.add(new KeyValue(key, value));
      }
      rwChannel.close();
      return keyValues;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<Object> readFile(String fileName, MessageType dataType,
                                      KryoMemorySerializer deserializer) {
    String outFileName = Paths.get(fileName).toString();
    FileChannel rwChannel;
    try {
      rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_ONLY, 0, rwChannel.size());

      List<Object> values = new ArrayList<>();
      // lets read the key values
      int totalRead = 0;
      while (totalRead < rwChannel.size()) {
        Object value;

        int dataSize = os.getInt();
        value = deserialize(dataType, deserializer, os, dataSize);
        values.add(value);
      }
      rwChannel.close();
      return values;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Object deserialize(MessageType dataType, KryoMemorySerializer deserializer,
                                    ByteBuffer os, int dataSize) {
    Object data = null;
    if (dataType == MessageType.OBJECT) {
      byte[] bytes = new byte[dataSize];
      os.get(bytes);
      data = deserializer.deserialize(bytes);
    } else if (dataType == MessageType.BYTE) {
      byte[] bytes = new byte[dataSize];
      os.get(bytes);
      data = bytes;
    } else if (dataType == MessageType.DOUBLE) {
      double[] bytes = new double[dataSize];
      for (int i = 0; i < dataSize; i++) {
        bytes[i] = os.getDouble();
      }
      data = bytes;
    } else if (dataType == MessageType.INTEGER) {
      int[] bytes = new int[dataSize];
      for (int i = 0; i < dataSize; i++) {
        bytes[i] = os.getInt();
      }
      data = bytes;
    } else if (dataType == MessageType.LONG) {
      long[] bytes = new long[dataSize];
      for (int i = 0; i < dataSize; i++) {
        bytes[i] = os.getLong();
      }
      data = bytes;
    } else if (dataType == MessageType.SHORT) {
      short[] bytes = new short[dataSize];
      for (int i = 0; i < dataSize; i++) {
        bytes[i] = os.getShort();
      }
      data = bytes;
    } else if (dataType == MessageType.CHAR) {
      char[] bytes = new char[dataSize];
      for (int i = 0; i < dataSize; i++) {
        bytes[i] = os.getChar();
      }
      data = bytes;
    }
    return data;
  }

  /**
   * Save the list of records to the file system
   * @param outFileName out file name
   */
  public static void saveSizes(List<Integer> sizes, String outFileName) {
    try {
      FileChannel rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, sizes.size() * 4);
      for (Integer s : sizes) {
        os.putInt(s);
      }
      rwChannel.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed write to disc", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Open a saved part and load it as a memory mapped file
   * @param fileName name of the file
   * @return the bytebuffer and the file channel
   */
  public static OpenFile openSavedPart(String fileName) {
    String outFileName = Paths.get(fileName).toString();
    FileChannel rwChannel;
    try {
      rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_ONLY, 0, rwChannel.size());
      return new OpenFile(rwChannel, os);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Open a saved part and load it as a memory mapped file
   * @param fileName name of the file
   * @param position the position to start reading
   * @param length length of the bytes to read
   * @return the bytebuffer and the file channel
   */
  public static OpenFile openSavedPart(String fileName, long position, long length) {
    String outFileName = Paths.get(fileName).toString();
    FileChannel rwChannel;
    try {
      rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_ONLY, position, length);
      return new OpenFile(rwChannel, os);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
