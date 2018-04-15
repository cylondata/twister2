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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageType;

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
  public static void saveKeyValues(List<KeyValue> records, List<Integer> sizes,
                                   long size, String outFileName, MessageType keyType) {
    try {
      FileChannel rwChannel = new RandomAccessFile(outFileName, "rw").getChannel();
      ByteBuffer os = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
      for (int i = 0; i < records.size(); i++) {
        Object key = records.get(i).getKey();

        byte[] r = (byte[]) records.get(i).getValue();
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
