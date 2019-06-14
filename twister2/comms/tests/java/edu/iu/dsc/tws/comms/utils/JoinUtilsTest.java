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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.threading.CommonThreadPool;
import edu.iu.dsc.tws.comms.api.JoinedTuple;
import edu.iu.dsc.tws.comms.api.MessageTypes;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.comms.shuffle.FSKeyedSortedMerger2;
import edu.iu.dsc.tws.comms.shuffle.RestorableIterator;

public class JoinUtilsTest {

  private static final Logger LOG = Logger.getLogger(JoinUtilsTest.class.getName());

  @Test
  public void innerJoin() {
    List<Tuple> left = new ArrayList<>();
    List<Tuple> right = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < 10000; i++) {
      left.add(Tuple.of(random.nextInt(10), random.nextInt()));
      right.add(Tuple.of(random.nextInt(10), random.nextInt()));
    }
//    left.add(Tuple.of(1, 10));
//    left.add(Tuple.of(2, 201));
//    left.add(Tuple.of(2, 202));
//    left.add(Tuple.of(3, 30));
//
//    right.add(Tuple.of(1, 100));
//    right.add(Tuple.of(2, 2001));
//    right.add(Tuple.of(2, 2002));


    FSKeyedSortedMerger2 fsk1 = new FSKeyedSortedMerger2(
        10,
        100,
        "/tmp",
        "op-1-" + UUID.randomUUID().toString(),
        MessageTypes.INTEGER,
        MessageTypes.INTEGER,
        (Comparator<Integer>) Integer::compare,
        0,
        false,
        1
    );

    for (Tuple tuple : left) {
      byte[] data = MessageTypes.INTEGER.getDataPacker()
          .packToByteArray((Integer) tuple.getValue());
      fsk1.add(tuple.getKey(), data, data.length);
      fsk1.run();
    }

    FSKeyedSortedMerger2 fsk2 = new FSKeyedSortedMerger2(
        10,
        100,
        "/tmp",
        "op-2-" + UUID.randomUUID().toString(),
        MessageTypes.INTEGER,
        MessageTypes.INTEGER,
        (Comparator<Integer>) Integer::compare,
        0,
        false,
        1
    );

    for (Tuple tuple : right) {
      byte[] data = MessageTypes.INTEGER.getDataPacker()
          .packToByteArray((Integer) tuple.getValue());
      fsk2.add(tuple.getKey(), data, data.length);
      fsk2.run();
    }

    CommonThreadPool.init(Config.newBuilder().build());

    fsk1.switchToReading();
    fsk2.switchToReading();

    Iterator iterator = JoinUtils.innerJoin(
        (RestorableIterator) fsk1.readIterator(),
        (RestorableIterator) fsk2.readIterator(),
        new KeyComparatorWrapper((Comparator<Integer>) Integer::compare)
    );


    List<Object> objects = JoinUtils.innerJoin(left, right,
        new KeyComparatorWrapper(Comparator.naturalOrder()));

    objects.sort(Comparator.comparingInt(o -> (Integer) ((JoinedTuple) o).getKey()));


    int i = 0;
    while (iterator.hasNext()) {
      JoinedTuple nextFromIt = (JoinedTuple) iterator.next();
      JoinedTuple nextFromList = (JoinedTuple) objects.get(i++);

      Assert.assertEquals(nextFromIt.getKey(), nextFromList.getKey());
    }


    Assert.assertEquals(i, objects.size());

    LOG.info(objects.toString());
  }
}
