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

import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.structs.Tuple;

/**
 * Wrapper for tuples in memory.
 */
public class ControlledMemoryReader implements ControlledReader<Tuple> {
  private static final Logger LOG = Logger.getLogger(ControlledMemoryReader.class.getName());

  /**
   * We use the key comparator to sort the keys
   */
  private Comparator keyComparator;

  /**
   * the current read index
   */
  private int readIndex;

  /**
   * The
   */
  private int restoredIndex;

  /**
   * The in-memory message
   */
  private List<Tuple> inMemoryMessages;

  public ControlledMemoryReader(List<Tuple> messages,
                                Comparator keyComparator) {
    this.keyComparator = keyComparator;
    this.readIndex = 0;
    this.inMemoryMessages = messages;
    this.restoredIndex = -1;
  }

  public void open() {
  }

  public void releaseResources() {
  }

  @Override
  public boolean hasNext() {
    return readIndex < inMemoryMessages.size();
  }

  public Object nextKey() {
    Tuple t = inMemoryMessages.get(readIndex);
    return t.getKey();
  }

  @Override
  public Tuple next() {
    if (!this.hasNext()) {
      throw new NoSuchElementException("No more keys to iterate");
    }

    Tuple tuple = inMemoryMessages.get(readIndex);
    readIndex++;

    return tuple;
  }

  @Override
  public void createRestorePoint() {
    restoredIndex = readIndex;
  }

  @Override
  public void restore() {
    readIndex = restoredIndex;
  }

  @Override
  public void clearRestorePoint() {
    this.restoredIndex = -1;
  }

  @Override
  public boolean hasRestorePoint() {
    return restoredIndex >= 0;
  }

  @Override
  public int compareTo(ControlledReader<Tuple> o) {
    // deliberately not checking null. If we are getting null here, check the code of this class and
    // FSKeyedSortedMerger class.
    return this.keyComparator.compare(this.nextKey(), o.nextKey());
  }
}
