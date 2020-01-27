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

package edu.iu.dsc.tws.comms.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.UUID;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.dataset.partition.BufferedCollectionPartition;
import edu.iu.dsc.tws.dataset.partition.DiskBackedCollectionPartition;

public class DiskBasedList implements List {

  private BufferedCollectionPartition collectionPartition;
  private MessageType dataType;
  private int size = 0;

  private DataPartitionConsumer currentConsumer;
  private int consumingIndex = -1;

  public DiskBasedList(Config conf,
                       MessageType dataType) {
    long maxBytesInMemory = CommunicationContext.getShuffleMaxBytesInMemory(conf);
    long maxRecordsInMemory = CommunicationContext.getShuffleMaxRecordsInMemory(conf);

    this.collectionPartition = new DiskBackedCollectionPartition<>(maxRecordsInMemory,
        dataType, maxBytesInMemory, conf, UUID.randomUUID().toString());
    this.dataType = dataType;
  }

  private Twister2RuntimeException unSupportedException() {
    return new Twister2RuntimeException("Unsupported disk backed operation");
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public boolean contains(Object o) {
    throw unSupportedException();
  }

  @Override
  public boolean add(Object t) {
    this.collectionPartition.add(t);
    this.size++;
    return true;
  }

  @Override
  public boolean remove(Object o) {
    throw unSupportedException();
  }

  @Override
  public boolean containsAll(Collection collection) {
    throw unSupportedException();
  }

  @Override
  public boolean addAll(Collection data) {
    for (Object datum : data) {
      this.add(datum);
    }
    return true;
  }

  @Override
  public boolean addAll(int i, Collection collection) {
    throw unSupportedException();
  }

  @Override
  public void clear() {
    this.collectionPartition.clear();
  }

  public void dispose() {
    this.collectionPartition.dispose();
  }

  @Override
  public Object get(int i) {
    if (this.consumingIndex > i || this.currentConsumer == null) {
      this.currentConsumer = this.collectionPartition.getConsumer();
      this.consumingIndex = 0;
    }
    while (this.consumingIndex++ < i && currentConsumer.hasNext()) {
      currentConsumer.next();
    }
    if (currentConsumer.hasNext()) {
      return currentConsumer.next();
    }
    return null;
  }

  @Override
  public Object set(int i, Object o) {
    throw unSupportedException();
  }

  @Override
  public void add(int i, Object o) {
    throw unSupportedException();
  }

  @Override
  public Object remove(int i) {
    throw unSupportedException();
  }

  @Override
  public int indexOf(Object o) {
    throw unSupportedException();
  }

  @Override
  public int lastIndexOf(Object o) {
    throw unSupportedException();
  }

  @Override
  public ListIterator listIterator() {
    throw unSupportedException();
  }

  @Override
  public ListIterator listIterator(int i) {
    throw unSupportedException();
  }

  @Override
  public List subList(int i, int i1) {
    throw unSupportedException();
  }

  @Override
  public boolean retainAll(Collection collection) {
    throw unSupportedException();
  }

  @Override
  public boolean removeAll(Collection collection) {
    throw unSupportedException();
  }

  @Override
  public Iterator iterator() {
    final DataPartitionConsumer<byte[]> consumer = this.collectionPartition.getConsumer();
    return new Iterator() {
      @Override
      public boolean hasNext() {
        return consumer.hasNext();
      }

      @Override
      public Object next() {
        byte[] next = consumer.next();
        return dataType.getDataPacker().unpackFromByteArray(next);
      }
    };
  }

  @Override
  public Object[] toArray() {
    throw unSupportedException();
  }

  @Override
  public Object[] toArray(Object[] objects) {
    throw unSupportedException();
  }
}
