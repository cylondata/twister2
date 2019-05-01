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
package edu.iu.dsc.tws.common.util;

import org.junit.Assert;
import org.junit.Test;

public class IterativeLinkedListTest {
  @Test
  public void testIterator() {
    IterativeLinkedList<Integer> list = new IterativeLinkedList<>();
    for (int i = 0; i < 10; i++) {
      list.add(i);
    }

    IterativeLinkedList.ILLIterator itr = list.iterator();
    int expected = 0;
    while (itr.hasNext()) {
      Assert.assertEquals(expected++, itr.next());
    }
  }
}
