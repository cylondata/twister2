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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.io.types.DataDeserializer;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;

/**
 * Save the records to file system and retrieve them, this is just values, so no
 * sorting as in the keyed case
 */
public class FSMerger implements Shuffle {
  private static final Logger LOG = Logger.getLogger(FSMerger.class.getName());

  /**
   * Maximum bytes to keep in memory
   */
  private int maxBytesToKeepInMemory;

  /**
   * Maximum number of records in memory. We will choose lesser of two maxes to write to disk
   */
  private int maxRecordsInMemory;

  /**
   * The base folder to work on
   */
  private String folder;

  /**
   * Operation name
   */
  private String operationName;

  /**
   * No of files written to the disk so far
   * The files are started from 0 and go up to this amount
   */
  private int noOfFileWritten = 0;

  /**
   * The size of the records in memory
   */
  private List<Integer> bytesLength = new ArrayList<>();

  /**
   * List of bytes in the memory so far
   */
  private List<byte[]> bytesInMemory = new ArrayList<>();

  /**
   * The deserialized objects in memory
   */
  private List<Object> objectsInMemory = new ArrayList<>();

  /**
   * The number of total bytes in each file part written to disk
   */
  private List<Integer> filePartBytes = new ArrayList<>();

  /**
   * Amount of bytes in the memory
   */
  private long numOfBytesInMemory = 0;

  /**
   * The type of value to be returned
   */
  private MessageType valueType;

  private Lock lock = new ReentrantLock();
  private Condition notFull = lock.newCondition();

  /**
   * The kryo serializer
   */
  private KryoMemorySerializer kryoSerializer;

  private enum FSStatus {
    WRITING,
    READING
  }

  private FSStatus status = FSStatus.WRITING;

  public FSMerger(int maxBytesInMemory, int maxRecsInMemory,
                  String dir, String opName, MessageType vType) {
    this.maxBytesToKeepInMemory = maxBytesInMemory;
    this.maxRecordsInMemory = maxRecsInMemory;
    this.folder = dir;
    this.operationName = opName;
    this.valueType = vType;
    this.kryoSerializer = new KryoMemorySerializer();
  }

  /**
   * Add the data to the file
   */
  public void add(byte[] data, int length) {
    if (status == FSStatus.READING) {
      throw new RuntimeException("Cannot add after switching to reading");
    }

    lock.lock();
    try {
      bytesInMemory.add(data);
      bytesLength.add(length);

      numOfBytesInMemory += length;
      if (numOfBytesInMemory > maxBytesToKeepInMemory
          || bytesInMemory.size() > maxRecordsInMemory) {
        notFull.signal();
      }
    } finally {
      lock.unlock();
    }
  }

  public void switchToReading() {
    status = FSStatus.READING;
    deserializeObjects();
  }

  private void deserializeObjects() {
    for (int i = 0; i < bytesInMemory.size(); i++) {
      Object o = DataDeserializer.deserialize(valueType, kryoSerializer, bytesInMemory.get(i));
      objectsInMemory.add(o);
    }
  }

  /**
   * This method saves the data to file system
   */
  public void run() {
    lock.lock();
    try {
      // it is time to write
      if (numOfBytesInMemory > maxBytesToKeepInMemory
          || bytesInMemory.size() > maxRecordsInMemory) {
        // save the bytes to disk
        LOG.log(Level.FINE, String.format("Save objects bytes %d objects %d",
            numOfBytesInMemory, bytesInMemory.size()));
        FileLoader.saveObjects(bytesInMemory, bytesLength,
            numOfBytesInMemory, getSaveFileName(noOfFileWritten));

        bytesInMemory.clear();
        bytesLength.clear();
        noOfFileWritten++;
        numOfBytesInMemory = 0;
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * This method gives the values
   */
  public Iterator<Object> readIterator() {
    // lets start with first file
    return new FSIterator();
  }

  private class FSIterator implements Iterator<Object> {
    // the current file index
    private int currentFileIndex = -1;
    // Index of the current file
    private int currentIndex = 0;
    // the iterator for list of bytes in memory
    private Iterator<Object> it;
    // the current open values
    private List<Object> openValues;

    FSIterator() {
      it = objectsInMemory.iterator();
    }

    @Override
    public boolean hasNext() {
      // we are reading from in memory
      boolean next;
      if (currentFileIndex == -1) {
        next = it.hasNext();
        if (!next) {
          // we need to open the first file part
          if (noOfFileWritten > 0) {
            openFilePart();
          } else {
            // no file parts written, end of iteration
            return false;
          }
        } else {
          return true;
        }
      }

      if (currentFileIndex >= 0) {
        if (currentIndex < openValues.size()) {
          return true;
        } else {
          if (currentFileIndex < noOfFileWritten - 1) {
            openFilePart();
            return true;
          } else {
            return false;
          }
        }
      }
      return false;
    }

    private void openFilePart() {
      // lets read the bytes from the file
      currentFileIndex++;
      openValues = FileLoader.readFile(getSaveFileName(currentFileIndex),
          valueType, kryoSerializer);
      currentIndex = 0;
    }

    @Override
    public Object next() {
      // we are reading from in memory
      if (currentFileIndex == -1) {
        return it.next();
      }

      if (currentFileIndex >= 0) {
        Object data = openValues.get(currentIndex);
        currentIndex++;
        return data;
      }

      return null;
    }
  }

  /**
   * Cleanup the directories
   */
  public void clean() {
    File file = new File(getSaveFolderName());
    try {
      FileUtils.cleanDirectory(file);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to clear directory: " + file, e);
    }
  }

  /**
   * Get the file name to save the current part
   *
   * @return the save file name
   */
  private String getSaveFolderName() {
    return folder + "/" + operationName;
  }

  /**
   * Get the file name to save the current part
   *
   * @param filePart file part index
   * @return the save file name
   */
  private String getSaveFileName(int filePart) {
    return folder + "/" + operationName + "/part_" + filePart;
  }

  /**
   * Get the name of the sizes file name
   *
   * @param filePart file part index
   * @return filename
   */
  private String getSizesFileName(int filePart) {
    return folder + "/" + operationName + "/part_sizes_" + filePart;
  }
}
