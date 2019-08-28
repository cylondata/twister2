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
package edu.iu.dsc.tws.examples.tset;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.partition.CollectionPartition;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.tset.sources.CacheSource;
import static java.util.stream.Collectors.toList;

// todo: take this to the tset impl tests package
public class CacheSourceTest {
  private static final Logger LOG = Logger.getLogger(CacheSourceTest.class.getName());

  private int i;

  public CacheSourceTest(int ii) {
    this.i = ii;
  }

  public static void main(String[] args) {
    LOG.info("AAA");

    DataObjectImpl<Integer> dObj = new DataObjectImpl<>(null);

    CollectionPartition<Integer> p0 = new CollectionPartition<>(0);
    p0.addAll(IntStream.range(0, 10).boxed().collect(toList()));

    CollectionPartition<Integer> p1 = new CollectionPartition<>(1);
    p1.addAll(IntStream.range(10, 25).boxed().collect(toList()));

    dObj.addPartition(p0);
    dObj.addPartition(p1);

    CacheSource<Integer> src = new CacheSource<>(dObj);

    List<Integer> trusted = IntStream.range(0, 25).boxed().collect(toList());
    List<Integer> result = new ArrayList<>();
    check(src, trusted, result);

    dObj = new DataObjectImpl<>(null);

    EntityPartition<Integer> e0 = new EntityPartition<>(0, 100);
    EntityPartition<Integer> e1 = new EntityPartition<>(1, 101);

    dObj.addPartition(e0);
    dObj.addPartition(e1);

    src = new CacheSource<>(dObj);

    trusted = IntStream.range(100, 102).boxed().collect(toList());
    result = new ArrayList<>();
    check(src, trusted, result);
  }

  private static void check(CacheSource<Integer> src, List<Integer> trusted, List<Integer> result) {
    // dummy has next calls
    src.hasNext();
    src.hasNext();
    src.hasNext();

    while (src.hasNext()) {
      result.add(src.next());
    }

    if (!trusted.equals(result)) {
      LOG.info(trusted.toString());
      LOG.info(result.toString());
      throw new RuntimeException("test failure!");
    } else {
      LOG.info("success!");
    }
  }
}
