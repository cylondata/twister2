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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.threading.CommonThreadPool;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;

/**
 * Sorted merger implementation
 * todo add support to handling large values. When tuples have large values, since we are
 * opening multiple files at the same time, when reading, this implementation overloads heap
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class FSKeyedSortedMerger2 implements Shuffle {
  private static final Logger LOG = Logger.getLogger(FSKeyedSortedMerger2.class.getName());
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
   * List of bytes in the memory so far
   */
  private LinkedList<Tuple> recordsInMemory;

  /**
   * The deserialized objects in memory
   */
  private List<Tuple> objectsInMemory = new ArrayList<>();

  /**
   * Maximum size of a tuple written to disk in each file
   */
  private AtomicLong largestTupleSizeRecorded = new AtomicLong(Long.MIN_VALUE);

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
  private Comparator keyComparator;

  private ComparatorWrapper comparatorWrapper;
  private ListComparatorWrapper listComparatorWrapper;

  private volatile Semaphore fileWriteLock = new Semaphore(1);

  /**
   * The id of the task
   */
  private int target;

  private enum FSStatus {
    WRITING,
    READING,
    DONE
  }

  private FSStatus status = FSStatus.WRITING;

  /**
   * Create a key based sorted merger
   */
  public FSKeyedSortedMerger2(int maxBytesInMemory, int maxRecsInMemory,
                              String dir, String opName, MessageType kType,
                              MessageType dType, Comparator kComparator, int tar) {
    this.maxBytesToKeepInMemory = maxBytesInMemory;
    this.maxRecordsInMemory = maxRecsInMemory;

    //we can expect atmost this much of unique keys
    this.recordsInMemory = new LinkedList<>();

    this.folder = dir;
    this.operationName = opName;
    this.keyType = kType;
    this.dataType = dType;
    this.keyComparator = kComparator;
    this.comparatorWrapper = new ComparatorWrapper(keyComparator);
    this.listComparatorWrapper = new ListComparatorWrapper(this.comparatorWrapper);

    this.target = tar;
    LOG.info("Disk merger configured. Folder : " + folder
        + ", Bytes in memory :" + maxBytesInMemory + ", Records in memory : " + maxRecsInMemory);
  }

  /**
   * Add the data to the file
   */
  public synchronized void add(Object key, byte[] data, int length) {
    if (status == FSStatus.READING) {
      throw new RuntimeException("Cannot add after switching to reading");
    }

    this.recordsInMemory.add(new Tuple(key, data));

    // todo ignoring length for now
    // this.bytesLength.add(length);
    this.numOfBytesInMemory += data.length;
  }

  public synchronized void switchToReading() {
    LOG.info("Switching to read...");
    try {
      fileWriteLock.acquire();
    } catch (InterruptedException iex) {
      LOG.log(Level.SEVERE, "Couldn't switch to reading", iex);
    }
    try {
      LOG.info(String.format("Reading from %d files", noOfFileWritten));
      status = FSStatus.READING;
      // lets convert the in-memory data to objects
      deserializeObjects();
      // lets sort the in-memory objects
      //todo can be improved
      objectsInMemory.sort(this.comparatorWrapper);
    } finally {
      fileWriteLock.release();
    }
  }

  /**
   * Wrapper for comparing KeyValue with the user defined comparator
   */
  private class ComparatorWrapper implements Comparator<Tuple> {
    private Comparator comparator;

    ComparatorWrapper(Comparator com) {
      this.comparator = com;
    }

    @Override
    public int compare(Tuple o1, Tuple o2) {
      return comparator.compare(o1.getKey(), o2.getKey());
    }
  }

  /**
   * Wrapper for comparing KeyValue with the user defined comparator
   */
  private class ListComparatorWrapper implements Comparator<List<Tuple>> {
    private ComparatorWrapper comparator;

    ListComparatorWrapper(ComparatorWrapper com) {
      this.comparator = com;
    }

    @Override
    public int compare(List<Tuple> o1, List<Tuple> o2) {
      return this.comparator.compare(o1.get(0), o2.get(0));
    }
  }

  private void deserializeObjects() {
    int threads = CommonThreadPool.getThreadCount() + 1; //this thread is also counted
    List<Future<Boolean>> deserializeFutures = new ArrayList<>();

    int chunkSize = this.recordsInMemory.size() / threads;

    if (this.recordsInMemory.size() % threads != 0) {
      chunkSize++;
    }

    for (int i = 0; i < this.recordsInMemory.size(); i += chunkSize) {
      final int start = i;
      final int end = Math.min(this.recordsInMemory.size(), i + chunkSize);
      //last chunk will be processed in this thread
      if (end == this.recordsInMemory.size()) {
        for (int j = start; j < end; j++) {
          Tuple tuple = recordsInMemory.get(j);
          Object o = dataType.getDataPacker().unpackFromByteArray((byte[]) tuple.getValue());
          tuple.setValue(o);
        }
      } else {
        deserializeFutures.add(CommonThreadPool.getExecutor().submit(() -> {
          for (int j = start; j < end; j++) {
            Tuple tuple = recordsInMemory.get(j);
            Object o = dataType.getDataPacker().unpackFromByteArray((byte[]) tuple.getValue());
            tuple.setValue(o);
          }
          return true;
        }));
      }
    }

    for (int i = 0; i < deserializeFutures.size(); i++) {
      try {
        deserializeFutures.get(i).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException("Error in deserializing records in memory", e);
      }
    }
  }

  /**
   * This method saves the data to file system
   */
  public synchronized void run() {
    // it is time to write
    if (numOfBytesInMemory >= maxBytesToKeepInMemory
        || this.recordsInMemory.size() >= maxRecordsInMemory) {


      // save the bytes to disk
      this.writeToFile();

      numOfBytesInMemory = 0;

      //making previous things garbage collectible
      this.recordsInMemory = new LinkedList<>();
      noOfFileWritten++;
    }
  }

  private void writeToFile() {
    try {
      if (this.fileWriteLock.availablePermits() == 0) {
        LOG.warning("Communication thread blocks on disk IO thread!");
      }
      this.fileWriteLock.acquire(); // allow 1 parallel write to disk

      //create references to existing data
      final LinkedList<Tuple> referenceToRecordsInMemory = this.recordsInMemory;
      final String fileName = getSaveFileName(noOfFileWritten);
      final long bytesInMemory = numOfBytesInMemory;

      CommonThreadPool.getExecutor().execute(() -> {
        try {
          // do the sort
          referenceToRecordsInMemory.sort(comparatorWrapper);

          long largestTupleWritten = FileLoader.saveKeyValues(
              referenceToRecordsInMemory, bytesInMemory, fileName, keyType);
          //todo get inside set?
          largestTupleSizeRecorded.set(
              Math.max(largestTupleSizeRecorded.get(), largestTupleWritten)
          );
        } finally {
          fileWriteLock.release();
        }
      });
    } catch (RejectedExecutionException regex) {
      LOG.log(Level.SEVERE, "Couldn't submit async write task", regex);
      fileWriteLock.release();
    } catch (InterruptedException e) {
      LOG.log(Level.SEVERE, "Couldn't write to the file", e);
      fileWriteLock.release();
    }
  }

  /**
   * This method gives the values
   */
  public Iterator<Object> readIterator() {
    try {
      return new Iterator<Object>() {

        private FSIterator fsIterator = new FSIterator();
        private Tuple nextTuple = fsIterator.hasNext() ? fsIterator.next() : null;
        private Iterator itOfCurrentKey = null;

        private void skipKeys() {
          //user is trying to skip keys. For now, we are iterating over them internally
          if (this.itOfCurrentKey != null) {
            //todo replace with an alternative approach
            while (this.itOfCurrentKey.hasNext()) {
              this.itOfCurrentKey.next();
            }
          }
        }

        @Override
        public boolean hasNext() {
          this.skipKeys();
          return nextTuple != null;
        }

        @Override
        public Tuple<Object, Iterator> next() {
          if (!hasNext()) {
            throw new NoSuchElementException("There are no more keys to iterate");
          }
          final Object currentKey = nextTuple.getKey();
          this.itOfCurrentKey = new Iterator<Object>() {
            @Override
            public boolean hasNext() {
              return nextTuple != null && nextTuple.getKey().equals(currentKey);
            }

            @Override
            public Object next() {
              if (this.hasNext()) {
                Object returnValue = nextTuple.getValue();
                if (fsIterator.hasNext()) {
                  nextTuple = fsIterator.next();
                } else {
                  nextTuple = null;
                }
                return returnValue;
              } else {
                throw new NoSuchElementException("There are no more values for key "
                    + currentKey);
              }
            }
          };
          Tuple<Object, Iterator> nextValueSet = new Tuple<>();
          nextValueSet.setKey(currentKey);
          nextValueSet.setValue(this.itOfCurrentKey);
          return nextValueSet;
        }
      };
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Error in creating iterator", e);
      throw new RuntimeException(e);
    }
  }

  private class FSIterator implements Iterator<Object> {


    private PriorityQueue<ControlledFileReader> controlledFileReaders
        = new PriorityQueue<>(1 + noOfFileWritten);
    private ControlledFileReader sameKeyReader;

    FSIterator() {
      ControlledFileReaderFlags meta = new ControlledFileReaderFlags(
          Math.max(numOfBytesInMemory, largestTupleSizeRecorded.get()),
          keyComparator
      );
      if (!objectsInMemory.isEmpty()) {
        ControlledFileReader inMemoryReader = ControlledFileReader.loadInMemory(
            meta, objectsInMemory, keyComparator);
        if (inMemoryReader.hasNext()) {
          this.controlledFileReaders.add(inMemoryReader);
        }
      }

      for (int i = 0; i < noOfFileWritten; i++) {
        ControlledFileReader fr = new ControlledFileReader(
            meta,
            getSaveFileName(i),
            keyType,
            dataType,
            keyComparator
        );
        if (fr.hasNext()) {
          controlledFileReaders.add(fr);
        } else {
          //done with this file
          fr.releaseResources();
          LOG.warning("Found a controlled file reader without any data");
        }
      }
    }

    @Override
    public boolean hasNext() {
      return this.sameKeyReader != null || !controlledFileReaders.isEmpty();
    }

    @Override
    public Tuple next() {
      ControlledFileReader fr = this.sameKeyReader;
      if (fr == null || !fr.hasNext()) {
        fr = this.controlledFileReaders.poll();
        fr.open();
      }
      Tuple nextTuple = fr.next();
      if (fr.hasNext() && nextTuple.getKey().equals(fr.nextKey())) {
        this.sameKeyReader = fr;
      } else if (fr.hasNext()) {
        this.sameKeyReader = null;
        this.controlledFileReaders.add(fr);
      } else {
        //done with this file
        this.sameKeyReader = null;
        fr.releaseResources();
      }
      return nextTuple;
    }
  }

  /**
   * Cleanup the directories
   */
  public void clean() {
    for (int i = 0; i < noOfFileWritten; i++) {
      File file = new File(getSaveFileName(i));
      if (file.exists()) {
        boolean deleted = file.delete();
        if (!deleted) {
          LOG.warning("Couldn't delete file : " + file.getName());
        }
      }
    }
    File rootFolder = new File(this.getSaveFolderName());
    rootFolder.deleteOnExit();
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
    return this.getSaveFolderName() + "/part_" + filePart;
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
