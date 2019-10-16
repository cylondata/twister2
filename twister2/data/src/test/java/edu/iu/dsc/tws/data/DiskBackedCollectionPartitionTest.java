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

    try (BufferedCollectionPartition<Integer> dbp = new DiskBackedCollectionPartition<>(
        10, MessageTypes.INTEGER, 1000,
        ConfigLoader.loadTestConfig())) {

      List<Integer> rawData = new ArrayList<>();

      for (int i = 0; i < 1000000 / Integer.BYTES; i++) {
        rawData.add(i);
        dbp.add(i);
      }

      DataPartitionConsumer<Integer> consumer = dbp.getConsumer();
      Iterator<Integer> rawIterator = rawData.iterator();
      while (consumer.hasNext()) {
        Assert.assertEquals(consumer.next(), rawIterator.next());
      }
      dbp.clear();
    }
  }
}
