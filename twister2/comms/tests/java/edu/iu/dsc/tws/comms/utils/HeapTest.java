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

import java.util.Comparator;

import org.junit.Assert;

import org.junit.Test;

import edu.iu.dsc.tws.comms.shuffle.KeyValue;

public class HeapTest {

  @Test
  public void insert() {
    Heap<Integer, Integer> heap = new Heap<>(7, Comparator.comparingInt(o -> o));
    heap.insert(new KeyValue<>(15, 15), 1);
    heap.insert(new KeyValue<>(6, 6), 1);
    heap.insert(new KeyValue<>(17, 17), 1);
    heap.insert(new KeyValue<>(7, 7), 1);
    heap.insert(new KeyValue<>(10, 10), 1);
    heap.insert(new KeyValue<>(12, 12), 1);
    heap.insert(new KeyValue<>(5, 5), 1);

    Assert.assertArrayEquals(new int[]{5, 7, 6, 15, 10, 17, 12},
        heap.getRawArray().stream().mapToInt(HeapNode::getKey).toArray());
  }

  @Test
  public void extractMin() {
    Heap<Integer, Integer> heap = new Heap<>(7, Comparator.comparingInt(o -> o));
    heap.insert(new KeyValue<>(15, 15), 1);
    heap.insert(new KeyValue<>(6, 6), 1);
    heap.insert(new KeyValue<>(17, 17), 1);
    heap.insert(new KeyValue<>(5, 5), 1);
    heap.insert(new KeyValue<>(7, 7), 1);
    heap.insert(new KeyValue<>(10, 10), 1);
    heap.insert(new KeyValue<>(12, 12), 1);

    Assert.assertEquals(5, (long) heap.extractMin().getKey());
    Assert.assertEquals(6, (long) heap.extractMin().getKey());
    Assert.assertEquals(7, (long) heap.extractMin().getKey());
    Assert.assertEquals(10, (long) heap.extractMin().getKey());
    Assert.assertEquals(12, (long) heap.extractMin().getKey());
    Assert.assertEquals(15, (long) heap.extractMin().getKey());
    Assert.assertEquals(17, (long) heap.extractMin().getKey());
    Assert.assertNull(heap.extractMin());
  }
}
