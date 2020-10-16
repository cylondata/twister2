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
package edu.iu.dsc.tws.dl.graph;

import edu.iu.dsc.tws.dl.utils.Util;

import java.util.*;

public class BFS<K> implements Iterator<Node<K>> {

  private Queue<Node<K>> queue = new ArrayDeque<>();
  private HashSet<Node<K>> visited = new HashSet<Node<K>>();
  private boolean reverse = false;

  public BFS(Node<K> source, boolean isReverse) {
    queue.add(source);
    this.reverse = isReverse;
  }

  @Override
  public boolean hasNext() {
    return queue.isEmpty();
  }

  @Override
  public Node<K> next() {
    Util.require(hasNext(), "No more elements in the graph");
    Node<K> node = queue.poll();
    visited.add(node);
    List<Node<K>> nextNodes = (!reverse) ? node.nextNodes() : node.prevNodes();
    // to preserve order
    LinkedHashSet<Node<K>> nodesSet = new LinkedHashSet();
    nextNodes.addAll(nextNodes);

    for (Node<K> kNode : nodesSet) {
      if(!visited.contains(kNode)){
        if(!queue.contains(kNode)){
          queue.add(kNode);
        }
      }
    }
    return node;
  }
}
