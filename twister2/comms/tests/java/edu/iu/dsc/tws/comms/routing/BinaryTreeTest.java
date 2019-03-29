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
package edu.iu.dsc.tws.comms.routing;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.comms.api.TaskPlan;
import static edu.iu.dsc.tws.comms.routing.RoutingTestUtils.createTaskPlan;
import static edu.iu.dsc.tws.comms.routing.RoutingTestUtils.destinations;

public class BinaryTreeTest {
  @Test
  public void testUniqueTrees() {
    int workers = 1024;
    TaskPlan p = createTaskPlan(workers, 1, 0);

    Set<Integer> s = destinations(workers, 1);
    BinaryTree router = new BinaryTree(2,
        2, p, 0, s);

    Node root1 = router.buildInterGroupTree(0);
    Node root2 = router.buildInterGroupTree(0);

    for (int i = 0; i < workers; i++) {
      Node search1 = BinaryTree.search(root1, i);
      Node search2 = BinaryTree.search(root2, i);

      Assert.assertEquals(search1, search2);
    }
  }

  @Test
  public void testUniqueTrees2() {
    int workers = 256;
    TaskPlan p = createTaskPlan(workers, 1, 0);

    Set<Integer> s = destinations(workers, 1);
    BinaryTree router = new BinaryTree(2,
        2, p, 0, s);

    Node root1 = router.buildInterGroupTree(0);
    Node root2 = router.buildInterGroupTree(0);

    Node search1 = BinaryTree.search(root1, 255);
    Node search2 = BinaryTree.search(root2, 255);

    Assert.assertEquals(search1, search2);
  }

  @Test
  public void testUniqueTrees3() {
    int workers = 1024;
    TaskPlan p = createTaskPlan(workers, 1, 0);
    TaskPlan p2 = createTaskPlan(workers, 1, 10);

    Set<Integer> s = destinations(workers, 1);
    BinaryTree router = new BinaryTree(2,
        2, p, 0, s);
    BinaryTree router2 = new BinaryTree(2,
        2, p2, 0, s);

    Node root1 = router.buildInterGroupTree(0);
    Node root2 = router2.buildInterGroupTree(0);

    for (int i = 0; i < workers; i++) {
      Node search1 = BinaryTree.search(root1, i);
      Node search2 = BinaryTree.search(root2, i);

      Assert.assertEquals(search1, search2);
    }
  }
}
