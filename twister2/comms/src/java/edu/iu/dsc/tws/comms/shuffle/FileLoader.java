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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.comms.dfw.io.types.DataDeserializer;
import edu.iu.dsc.tws.comms.dfw.io.types.KeyDeserializer;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;

@SuppressWarnings({"unchecked", "rawtypes"})
public final class FileLoader {
  private static final Logger LOG = Logger.getLogger(FileLoader.class.getName());

  private FileLoader() {
  }

  /**
   * Save the list of records to the file system
   *
   * @param records records to be written
   * @param size total size of the records
   * @param outFileName out file name
   */
  public static void saveObjects(List<byte[]> records, List<Integer> sizes,
                                 long size, String outFileName) {
    try {
      LOG.fine("Saving file: " + outFileName);
      Files.createDirectories(Paths.get(outFileName).getParent());
      FileChannel rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      // we need to write the size of each message
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0,
          size + sizes.size() * 4);
      int total = 0;
      for (int i = 0; i < records.size(); i++) {
        byte[] r = records.get(i);
        total += sizes.get(i) + 4;
        os.putInt(sizes.get(i));
        os.put(r, 0, sizes.get(i));
      }
      rwChannel.force(true);
      rwChannel.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed write to disc", e);
      throw new RuntimeException(e);
    }
  }


  /**
   * Save the list of records to the file system
   *
   * @param records records to be written
   * @param size total size of the records
   * @param outFileName out file name
   */
  public static int saveKeyValues(List<Tuple> records, List<Integer> sizes,
                                  long size, String outFileName, MessageType keyType,
                                  KryoMemorySerializer serializer) {
    try {
      // first serialize keys
      int totalSize = 0;
      List<byte[]> byteKeys = new ArrayList<>();
      if (keyType == MessageType.OBJECT) {
        for (Tuple record : records) {
          byte[] data = serializer.serialize(record.getKey());
          totalSize += data.length + 4;
          byteKeys.add(data);
        }
      } else {
        for (Tuple record : records) {
          if (keyType == MessageType.BYTE) {
            totalSize += 1;
          } else if (keyType == MessageType.DOUBLE) {
            totalSize += 8;
          } else if (keyType == MessageType.INTEGER) {
            totalSize += 4;
          } else if (keyType == MessageType.LONG) {
            totalSize += 8;
          } else if (keyType == MessageType.CHAR) {
            totalSize += 1;
          } else if (keyType == MessageType.SHORT) {
            totalSize += 2;
          }
        }
      }

      long sum = 0;
      for (Integer s : sizes) {
        sum += s;
      }
      if (sum != size) {
        LOG.log(Level.WARNING, "Sum doesn't equal size: " + sum + " != " + size);
      }
      // we need to write the data lengths and key lengths
      totalSize += size + sum;

      Files.createDirectories(Paths.get(outFileName).getParent());
      FileChannel rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, totalSize);
      for (int i = 0; i < records.size(); i++) {
        Tuple keyValue = records.get(i);
        byte[] r = (byte[]) keyValue.getValue();
        // serialize key with its length
        if (keyType == MessageType.OBJECT) {
          byte[] src = byteKeys.get(i);
          os.putInt(src.length);
          os.put(src);
        } else if (keyType == MessageType.BYTE) {
          byte key = (byte) keyValue.getKey();
          os.put(key);
        } else if (keyType == MessageType.DOUBLE) {
          double kd = (double) keyValue.getKey();
          os.putDouble(kd);
        } else if (keyType == MessageType.INTEGER) {
          int kd = (int) keyValue.getKey();
          os.putInt(kd);
        } else if (keyType == MessageType.LONG) {
          long kd = (long) keyValue.getKey();
          os.putLong(kd);
        } else if (keyType == MessageType.CHAR) {
          char kd = (char) keyValue.getKey();
          os.putChar(kd);
        } else if (keyType == MessageType.SHORT) {
          short kd = (short) keyValue.getKey();
          os.putShort(kd);
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

  public static List<Tuple> readFile(String fileName, MessageType keyType,
                                        MessageType dataType, KryoMemorySerializer deserializer) {
    String outFileName = Paths.get(fileName).toString();
    FileChannel rwChannel;
    try {
      rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_ONLY, 0, rwChannel.size());

      List<Tuple> keyValues = new ArrayList<>();
      // lets read the key values
      int totalRead = 0;
      int count = 0;
      while (totalRead < rwChannel.size()) {
        Object key;
        Object value;

        // for object type we read the object bytes + 4
        int keySize = getKeySize(keyType, os);
        key = KeyDeserializer.deserialize(keyType, deserializer, os,
            keySize - Integer.BYTES);

        int dataSize = os.getInt();
        value = DataDeserializer.deserialize(dataType, deserializer, os, dataSize);
        keyValues.add(new Tuple(key, value));

        totalRead += 8 + keySize + dataSize;
        count++;
      }
      rwChannel.force(true);
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
      int count = 0;
      while (totalRead < rwChannel.size()) {
        Object value;

        int dataSize = os.getInt();
        value = DataDeserializer.deserialize(dataType, deserializer, os, dataSize);
        values.add(value);
        totalRead += 4 + dataSize;
        count++;
      }
      rwChannel.force(true);
      rwChannel.close();
      return values;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Triple<List<Tuple>, Long, Long> openFilePart(String fileName, long startOffSet,
                                                                int maxSize, MessageType keyType,
                                                                MessageType dataType,
                                                                KryoMemorySerializer deserializer) {
    List<Tuple> keyValues = new ArrayList<>();
    String outFileName = Paths.get(fileName).toString();
    FileChannel rwChannel;
    try {
      rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      long size = maxSize < rwChannel.size() - startOffSet
          ? maxSize : rwChannel.size() - startOffSet;
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_ONLY, startOffSet, size);

      int totalRead = 0;
      while (totalRead < size) {
        Object key;
        Object value;

        int keySize = os.getInt();
        key = DataDeserializer.deserialize(keyType, deserializer, os, keySize);

        // we cannot read further
        if (totalRead + keySize > size) {
          break;
        }

        int dataSize = os.getInt();
        value = DataDeserializer.deserialize(dataType, deserializer, os, dataSize);

        // we cannot read further
        if (totalRead + keySize + dataSize > size) {
          break;
        }

        keyValues.add(new Tuple(key, value));
        totalRead += 8 + keySize + dataSize;
      }
      rwChannel.close();
      return new ImmutableTriple<>(keyValues, totalRead + startOffSet, rwChannel.size());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Reads a file part upto max size from start offset
   *
   * @param fileName name
   * @param startOffSet start offset in bytes
   * @param maxSize max size to read
   * @param keyType key type
   * @param dataType data type
   * @param deserializer the deserializer
   * @return OpenFilePart with read information and null if fails
   */
  public static OpenFilePart openPart(String fileName, long startOffSet,
                                      int maxSize, MessageType keyType,
                                      MessageType dataType,
                                      KryoMemorySerializer deserializer) {
    List<Tuple> keyValues = new ArrayList<>();
    String outFileName = Paths.get(fileName).toString();
    FileChannel rwChannel;
    try {
      rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      long size = maxSize <= rwChannel.size() - startOffSet
          ? maxSize : rwChannel.size() - startOffSet;
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_ONLY, startOffSet, size);

      int totalRead = 0;
      while (totalRead < size) {
        Object key;
        Object value;

        // for object type we have to read the length of the bytes first
        if (keyType == MessageType.OBJECT && totalRead + 4 > size) {
          break;
        }

        // for object type we read the object bytes + 4
        int keySize = getKeySize(keyType, os);
        // we cannot read further
        if (totalRead + keySize > size) {
          break;
        }
        key = KeyDeserializer.deserialize(keyType, deserializer, os,
            keySize - Integer.BYTES);

        if (totalRead + keySize + 4 > size) {
          break;
        }

        int dataSize = os.getInt();
        // we cannot read further
        if (totalRead + keySize + dataSize + 4 > size) {
          break;
        }
        value = DataDeserializer.deserialize(dataType, deserializer, os, dataSize);

        keyValues.add(new Tuple(key, value));
        totalRead += 4 + keySize + dataSize;
      }
      int size1 = (int) rwChannel.size();
      rwChannel.close();
      return new OpenFilePart(keyValues, totalRead + (int) startOffSet,
          size1, fileName);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static int getKeySize(MessageType dataType, ByteBuffer os) {
    int size;
    if (dataType == MessageType.OBJECT) {
      size = os.getInt() + Integer.BYTES;
    } else if (dataType == MessageType.BYTE) {
      size = Byte.BYTES;
    } else if (dataType == MessageType.DOUBLE) {
      size = Double.BYTES;
    } else if (dataType == MessageType.INTEGER) {
      size = Integer.BYTES;
    } else if (dataType == MessageType.LONG) {
      size = Long.BYTES;
    } else if (dataType == MessageType.SHORT) {
      size = Short.BYTES;
    } else if (dataType == MessageType.CHAR) {
      size = Character.BYTES;
    } else {
      size = os.getInt() + Integer.BYTES;
    }
    return size;
  }

  /**
   * Save the list of records to the file system
   *
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
   *
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
   *
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

  public static Object convertKeyToArray(MessageType keyType, Object key) {
    if (keyType == MessageType.INTEGER) {
      return new int[]{(int) key};
    } else if (keyType == MessageType.SHORT) {
      return new short[]{(short) key};
    }
    return null;
  }
}
