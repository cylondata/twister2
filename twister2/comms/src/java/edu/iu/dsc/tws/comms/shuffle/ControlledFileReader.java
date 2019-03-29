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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.comms.dfw.io.types.DataDeserializer;
import edu.iu.dsc.tws.comms.dfw.io.types.KeyDeserializer;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;

public class ControlledFileReader implements Iterator, Comparable<ControlledFileReader> {

  private static final Logger LOG = Logger.getLogger(ControlledFileReader.class.getName());

  private final String filePath;
  private RandomAccessFile raf;
  private ControlledFileReaderFlags meta;
  private MappedByteBuffer buffer;
  private FileChannel channel;

  private MessageType keyType;
  private MessageType dataType;
  private KryoMemorySerializer deserializer;
  private Comparator keyComparator;

  private Queue<Object> keysQ = new LinkedList<>();
  private Queue<Object> valuesQ = new LinkedList<>();
  private Queue<Integer> valueSizeQ = new LinkedList<>();

  private long mappedTill = 0;

  private boolean inMemory = false;

  public ControlledFileReader(ControlledFileReaderFlags meta,
                              String filePath,
                              MessageType keyType,
                              MessageType dataType,
                              KryoMemorySerializer deserializer,
                              Comparator keyComparator) {
    this.filePath = filePath;
    this.meta = meta;
    this.deserializer = deserializer;
    this.keyComparator = keyComparator;
    this.keyType = keyType;
    this.dataType = dataType;

    if (filePath != null) {
      this.open();
      this.readNextKey();
    }
  }

  /**
   * This method will be used to create an instance with Tuples which are already in memory
   */
  public static ControlledFileReader loadInMemory(
      ControlledFileReaderFlags meta,
      List<Tuple> tuples,
      Comparator keyComparator) {
    ControlledFileReader cfr = new ControlledFileReader(
        meta,
        null,
        null,
        null,
        null,
        keyComparator
    );
    tuples.forEach(tuple -> {
      cfr.keysQ.add(tuple.getKey());
      cfr.valuesQ.add(tuple.getValue());
      cfr.valueSizeQ.add(0); //neglect in memory stuff from limits
    });
    cfr.inMemory = true;
    return cfr;
  }

  public void open() {
    try {
      if (buffer == null && !this.inMemory) {
        this.raf = new RandomAccessFile(filePath, "r");
        this.channel = raf.getChannel();
        this.buffer = channel.map(FileChannel.MapMode.READ_ONLY, this.mappedTill,
            this.channel.size() - this.mappedTill);
        this.meta.increaseMemMapLoad(this);
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Couldn't memory map file for reading", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * This method tries to do following things in order,
   * <ol>
   * <li>Try to unmamp, release memoery mapped buffer</li>
   * <li>Close file channels</li>
   * <li>Make this.buffer garbage collectible</li>
   * </ol>
   * <p>
   * If unmapping fails, we expect GC to take care of releasing resources.
   * But this could fail when we have a large heap space(mmap limit could hit before GC runs).
   * For those cases, only option is to increase max_map_count of OS.
   * </p>
   */
  public void releaseResources() {
    if (!this.inMemory && this.buffer != null) {
      this.mappedTill = this.mappedTill + this.buffer.position();
      boolean unmapped = false;
      try {
        unmapped = MemoryMapUtils.unMapBuffer(this.buffer);
      } catch (Exception e) {
        //do nothing
      }

      if (!unmapped) {
        LOG.log(Level.WARNING, () -> "Couldn't unmap buffer forcefully. "
            + "But there is a chance of happening this automatically with next GC cycle.");
      }

      try {
        this.buffer = null;
        this.channel.close();
        this.raf.close();
        this.raf = null;
        this.channel = null;
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Error in releasing resources", e);
      }
    }
  }

  private Object readNextKey() {
    if (!this.inMemory && this.buffer.hasRemaining()) {
      int nextKeySize = this.getNextKeySize();
      Object nextKey = KeyDeserializer.deserialize(
          this.keyType, this.deserializer, this.buffer, nextKeySize
      );
      this.keysQ.add(nextKey);
      return nextKey;
    }
    return null;
  }

  /**
   * This method reads the next value from file and increases the memory load
   */
  private Object readNextValue() {
    if (!this.inMemory && this.buffer.hasRemaining()) {
      int dataSize = this.buffer.getInt();
      Object nextValue = DataDeserializer.deserialize(
          dataType, deserializer, this.buffer, dataSize
      );
      this.valuesQ.add(nextValue);
      this.valueSizeQ.add(dataSize);

      //increase global load on memory
      this.meta.increaseMemoryLoad(dataSize);
      return nextValue;
    }
    return null;
  }

  @Override
  public boolean hasNext() {
    return !keysQ.isEmpty();
  }

  public Object nextKey() {
    return keysQ.peek();
  }

  @Override
  public Tuple next() {
    if (!this.hasNext()) {
      throw new NoSuchElementException("No more keys to iterate");
    }
    if (keysQ.size() > valuesQ.size()) {
      this.readNextValue();
    }
    if (keysQ.size() != valuesQ.size()) {
      throw new RuntimeException(
          "Amount of keys and values mismatch. Possible logic error."
              + " Keys : " + keysQ.size()
              + " Values : " + valuesQ.size()
      );
    }
    Object key = keysQ.poll();
    Object value = valuesQ.poll();

    Object nextKey = this.readNextKey();
    //read while ,
    // 1. We have more keys,
    // 2. It's the same key (else we need to save space for other files)
    // 3. Memory limit permits
    while (nextKey != null && nextKey.equals(key) && !meta.hasMemoryLimitReached()) {
      this.readNextValue();
      nextKey = this.readNextKey();
    }
    meta.decreaseMemoryLoad(valueSizeQ.poll());
    return new Tuple(key, value);
  }

  private int getNextKeySize() {
    if (keyType == MessageType.OBJECT) {
      return buffer.getInt();
    } else if (keyType.isPrimitive()) {
      return 0;
    } else {
      return buffer.getInt();
    }
  }

  @Override
  public int compareTo(ControlledFileReader o) {
    // deliberately not checking null. If we are getting null here, check the code of this class and
    // FSKeyedSortedMerger class.
    return this.keyComparator.compare(this.nextKey(), o.nextKey());
  }
}
