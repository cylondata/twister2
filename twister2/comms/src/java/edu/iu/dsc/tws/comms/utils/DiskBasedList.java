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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.dataset.partition.BufferedCollectionPartition;
import edu.iu.dsc.tws.dataset.partition.DiskBackedCollectionPartition;

public class DiskBasedList extends ArrayList {

  private BufferedCollectionPartition<byte[]> collectionPartition;
  private MessageType dataType;

  public DiskBasedList(Config conf,
                       MessageType dataType) {
    this.collectionPartition = new DiskBackedCollectionPartition<>(0,
        MessageTypes.BYTE_ARRAY, conf);
    this.dataType = dataType;
  }

  @Override
  public boolean add(Object t) {
    if (t instanceof byte[]) {
      this.collectionPartition.add((byte[]) t);
    } else {
      this.collectionPartition.add(this.dataType.getDataPacker().packToByteArray(t));
    }
    return true;
  }

  @Override
  public boolean addAll(Collection data) {
    for (Object datum : data) {
      this.add(datum);
    }
    return true;
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
}
