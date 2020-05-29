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
package edu.iu.dsc.tws.comms.selectors;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class HashingSelectorTest {
  @Test
  public void testNext() {
    HashingSelector selector = new HashingSelector();
    Set<Integer> sources = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      sources.add(i);
    }
    Set<Integer> dests = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      dests.add(i);
    }
    selector.prepare(null, sources, dests);

    Assert.assertEquals(selector.next(0, 1), 11);
    Assert.assertEquals(selector.next(0, 2), 12);
    Assert.assertEquals(selector.next(0, 222), 2);

    int[] ints = {0, 1, 2}; // hash 30817
    Assert.assertEquals(selector.next(0, ints), 7);
  }
}
