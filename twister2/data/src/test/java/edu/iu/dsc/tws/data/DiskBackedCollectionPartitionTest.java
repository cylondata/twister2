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
package edu.iu.dsc.tws.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.dataset.partition.BufferedCollectionPartition;
import edu.iu.dsc.tws.dataset.partition.DiskBackedCollectionPartition;

public class DiskBackedCollectionPartitionTest {

  @Test
  public void testIO() {

    List<Integer> rawData = new ArrayList<>();

    String reference;

    try (BufferedCollectionPartition<Integer> dbp = new DiskBackedCollectionPartition<>(
        MessageTypes.INTEGER, 1000,
        ConfigLoader.loadTestConfig())) {


      for (int i = 0; i < 10000 / Integer.BYTES; i++) {
        rawData.add(i);
        dbp.add(i);
      }

      DataPartitionConsumer<Integer> consumer = dbp.getConsumer();
      Iterator<Integer> rawIterator = rawData.iterator();
      this.verify(consumer, rawIterator);

      reference = dbp.getReference();
    }

    // reload and re verify
    try (BufferedCollectionPartition<Integer> dbp = new DiskBackedCollectionPartition<>(
        MessageTypes.INTEGER, 1000, ConfigLoader.loadTestConfig(), reference)) {
      DataPartitionConsumer<Integer> consumer = dbp.getConsumer();
      Iterator<Integer> rawIterator = rawData.iterator();
      this.verify(consumer, rawIterator);
    }

    //verify with index based random reads
    try (BufferedCollectionPartition<Integer> dbp = new DiskBackedCollectionPartition<>(
        MessageTypes.INTEGER, 1000, ConfigLoader.loadTestConfig(), reference)) {
      Random random = new Random(System.currentTimeMillis());
      for (int i = 0; i < 10000; i++) {
        int index = random.nextInt(rawData.size());
        Assert.assertEquals(rawData.get(index), dbp.get(index));
      }
    }
  }

  private void verify(DataPartitionConsumer<Integer> consumer, Iterator<Integer> rawIterator) {
    while (rawIterator.hasNext()) {
      Assert.assertEquals(rawIterator.next(), consumer.next());
    }
  }
}
