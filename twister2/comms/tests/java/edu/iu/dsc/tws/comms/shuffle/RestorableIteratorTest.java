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
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.util.CommonThreadPool;

public class RestorableIteratorTest {

  @Test
  public void test() {
    FSKeyedSortedMerger2 fsk2 = new FSKeyedSortedMerger2(
        10,
        1000,
        "/tmp",
        "op-2-" + UUID.randomUUID().toString(),
        MessageTypes.INTEGER,
        MessageTypes.INTEGER,
        (Comparator<Integer>) Integer::compare,
        0,
        true,
        1
    );

    for (int i = 0; i < 1000000; i++) {
      byte[] bytes = MessageTypes.INTEGER.getDataPacker().packToByteArray(i);
      fsk2.add(i, bytes, bytes.length);
      fsk2.run();
    }

    CommonThreadPool.init(Config.newBuilder().build());

    fsk2.switchToReading();

    RestorableIterator<Object> objectRestorableIterator = fsk2.readIterator();

    boolean pointCreated = false;
    boolean testDone = false;


    int i = 0;
    for (i = 0; i < 1000000; i++) {
      Assert.assertEquals(i, ((Tuple) objectRestorableIterator.next()).getKey());

      if (!pointCreated && i == 40000) {
        objectRestorableIterator.createRestorePoint();
        pointCreated = true;
      }


      if (pointCreated && i == 70000 && !testDone) {
        objectRestorableIterator.restore();
        objectRestorableIterator.clearRestorePoint();
        i = 40000;
        testDone = true;
      }
    }
  }
}
