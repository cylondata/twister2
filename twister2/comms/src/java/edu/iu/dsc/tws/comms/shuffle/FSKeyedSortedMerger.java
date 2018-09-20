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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.io.types.DataDeserializer;
import edu.iu.dsc.tws.comms.utils.Heap;
import edu.iu.dsc.tws.comms.utils.HeapNode;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;

@SuppressWarnings({"unchecked", "rawtypes"})
public class FSKeyedSortedMerger implements Shuffle {
  private static final Logger LOG = Logger.getLogger(FSKeyedSortedMerger.class.getName());
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
  private List<KeyValue> recordsInMemory = new ArrayList<>();

  /**
   * The deserialized objects in memory
   */
  private List<KeyValue> objectsInMemory = new ArrayList<>();

  /**
   * The number of total bytes in each file part written to disk
   */
  private List<Integer> filePartBytes = new ArrayList<>();

  /**
   * Amount of bytes in the memory
   */
  private long numOfBytesInMemory = 0;

  /**
   * The type of the key used
   */
  private MessageType keyType;

  /**
   * The data type to be returned, by default it is byte array
   */
  private MessageType dataType;

  /**
   * The key comparator used for comparing keys
   */
  private Comparator<Object> keyComparator;

  private Lock lock = new ReentrantLock();
  private Condition notFull = lock.newCondition();

  /**
   * The id of the task
   */
  private int target;

  /**
   * The kryo serializer
   */
  private KryoMemorySerializer kryoSerializer;

  private enum FSStatus {
    WRITING,
    READING,
    DONE
  }

  private FSStatus status = FSStatus.WRITING;

  /**
   * Create a key based sorted merger
   */
  public FSKeyedSortedMerger(int maxBytesInMemory, int maxRecsInMemory,
                             String dir, String opName, MessageType kType,
                             MessageType dType, Comparator<Object> kComparator, int tar) {
    this.maxBytesToKeepInMemory = maxBytesInMemory;
    this.maxRecordsInMemory = maxRecsInMemory;
    this.folder = dir;
    this.operationName = opName;
    this.keyType = kType;
    this.dataType = dType;
    this.keyComparator = kComparator;
    this.kryoSerializer = new KryoMemorySerializer();
    this.target = tar;
  }

  /**
   * Add the data to the file
   */
  public void add(Object key, byte[] data, int length) {
    if (status == FSStatus.READING) {
      throw new RuntimeException("Cannot add after switching to reading");
    }

    Object k1 = FileLoader.convertKeyToArray(keyType, key);
    // LOG.log(Level.INFO, "adding value: target: " + target + " " + recordsInMemory.size());
    lock.lock();
    try {
      recordsInMemory.add(new KeyValue(k1, data));
      bytesLength.add(length);

      numOfBytesInMemory += length;
      if (numOfBytesInMemory > maxBytesToKeepInMemory
          || recordsInMemory.size() > maxRecordsInMemory) {
        notFull.signal();
      }
    } finally {
      lock.unlock();
    }
  }

  public void switchToReading() {
    lock.lock();
    try {
      status = FSStatus.READING;
      // lets convert the in-memory data to objects
      deserializeObjects();
      // lets sort the in-memory objects
      Collections.sort(objectsInMemory, keyComparator);
    } finally {
      lock.unlock();
    }
  }

  private void deserializeObjects() {
    for (int i = 0; i < recordsInMemory.size(); i++) {
      KeyValue kv = recordsInMemory.get(i);
      Object o = DataDeserializer.deserialize(dataType, kryoSerializer, (byte[]) kv.getValue());
      objectsInMemory.add(new KeyValue(kv.getKey(), o));
    }
  }

  /**
   * This method saves the data to file system
   */
  public void run() {
    List<KeyValue> list;
    lock.lock();
    try {
      // it is time to write
      if (numOfBytesInMemory > maxBytesToKeepInMemory
          || recordsInMemory.size() > maxRecordsInMemory) {
        list = recordsInMemory;
        recordsInMemory = new ArrayList<>();

        // first sort the values
        list.sort(keyComparator);

        // save the bytes to disk
        int totalSize = FileLoader.saveKeyValues(list, bytesLength,
            numOfBytesInMemory, getSaveFileName(noOfFileWritten), keyType, kryoSerializer);
        filePartBytes.add(totalSize);

        recordsInMemory.clear();
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
    // keep track of the open file
    private Map<Integer, OpenFilePart> openFiles = new HashMap<>();

    /**
     * Max memory per file
     */
    private int maxMemoryPerFile;

    /**
     * Heap to load the data from the files
     */
    private Heap heap;

    private int numValuesInHeap = 0;

    FSIterator() {
      heap = new Heap(noOfFileWritten + 1, keyComparator);
      // lets initialize the open files
      if (noOfFileWritten > 0) {
        maxMemoryPerFile = maxBytesToKeepInMemory / noOfFileWritten;
      } else {
        maxMemoryPerFile = maxBytesToKeepInMemory;
      }
      // lets open the files
      for (int i = 0; i < noOfFileWritten; i++) {
        OpenFilePart part = FileLoader.openPart(getSaveFileName(i), 0,
            maxMemoryPerFile, keyType, dataType, kryoSerializer);
        openFiles.put(i, part);
      }
      // lets insert the values in memory as -1 entry
      openFiles.put(-1, new OpenFilePart(objectsInMemory, 0, 0, null));

      // now lets get first value from each and insert to heap
      for (Map.Entry<Integer, OpenFilePart> e : openFiles.entrySet()) {
        OpenFilePart part = e.getValue();
        if (part.hasNext()) {
          KeyValue keyValue = part.next();

          heap.insert(keyValue, e.getKey());
          numValuesInHeap++;
        } else {
          LOG.log(Level.WARNING, String.format("File part without any initial values: %s target %d",
              part.getFileName(), target));
        }
      }
    }

    @Override
    public boolean hasNext() {
      // now lets get a value from the same list
      return numValuesInHeap > 0;
    }

    @Override
    public KeyValue next() {
      // we are reading from in memory
      HeapNode node = heap.extractMin();
      numValuesInHeap--;

      // now lets try to insert a value to heap
      OpenFilePart part = openFiles.get(node.listNo);
      // okay we don't have anything in this open file
      if (!part.hasNext()) {
        // we are done with this file
        if (part.getFileSize() <= part.getReadOffSet()) {
          openFiles.remove(node.listNo);
        } else {
          // we are not going to do this for -1, memory data
          OpenFilePart newPart = FileLoader.openPart(getSaveFileName(node.listNo),
              part.getReadOffSet(), maxMemoryPerFile, keyType, dataType, kryoSerializer);
          openFiles.put(node.listNo, newPart);

          if (newPart.hasNext()) {
            KeyValue next = newPart.next();
            heap.insert(next, node.listNo);
            numValuesInHeap++;
          }
        }
      } else {
        heap.insert(part.next(), node.listNo);
        numValuesInHeap++;
      }

      return node.data;
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
    status = FSStatus.DONE;
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
