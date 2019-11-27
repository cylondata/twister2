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
package edu.iu.dsc.tws.tset.graph;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.api.tset.TBase;

/**
 * DAG graph implementation
 *
 * @param <T> node type
 */
public class DAGMutableGraph<T extends TBase> implements MutableGraph<T> {

  private final boolean directed;
  private Map<String, T> index;
  private Map<String, Set<String>> childList;
  private Map<String, Set<String>> parentList;
  private boolean allowsSelfLoop;

  public DAGMutableGraph() {
    this.directed = true;
    this.allowsSelfLoop = false;
    index = new HashMap<>();
    childList = new HashMap<>();
    parentList = new HashMap<>();
  }

  @Override
  public boolean addNode(T node) {
    if (index.containsKey(node.getId())) {
      return false;
    }

    index.put(node.getId(), node);
    childList.put(node.getId(), new HashSet<>());
    parentList.put(node.getId(), new HashSet<>());
    return true;
  }

  @Override
  public boolean putEdge(T orgin, T target) {

    if (checkCycle(orgin, target)) {
      throw new IllegalStateException("This edge introduces an cycle to this graph");
    }
    addNode(orgin);
    addNode(target);
    if (childList.get(orgin.getId()).contains(target.getId())) {
      return false;
    }

    childList.get(orgin.getId()).add(target.getId());
    parentList.get(target.getId()).add(orgin.getId());
    return true;
  }

  @Override
  public Set<T> nodes() {
    return new HashSet<>(index.values());
  }

  @Override
  public boolean removeNode(T node) {
    if (!index.containsKey(node.getId())) {
      return false;
    }

    // remove the edges to the node from its parents
    Set<String> parentIds = parentList.remove(node.getId());
    for (String parent : parentIds) {
      childList.get(parent).remove(node.getId());
    }

    // remove this node from the parent list form all the child nodes
    Set<String> childIds = childList.remove(node.getId());
    for (String child : childIds) {
      parentList.get(child).remove(node.getId());
    }

    return true;
  }

  @Override
  public boolean removeEdge(T origin, T target) {
    if (!childList.get(origin.getId()).contains(target.getId())) {
      return false;
    }

    childList.get(origin.getId()).remove(target.getId());
    parentList.get(target.getId()).remove(origin.getId());
    return true;
  }

  @Override
  public Set<T> successors(T node) {
    Set<String> successorIds = childList.get(node.getId());
    Set<T> successors = new HashSet<>();
    for (String successorId : successorIds) {
      successors.add(index.get(successorId));
    }
    return successors;
  }

  @Override
  public Set<T> predecessors(T node) {
    Set<String> predecessorIds = parentList.get(node.getId());
    Set<T> predecessors = new HashSet<>();
    for (String predecessorId : predecessorIds) {
      predecessors.add(index.get(predecessorId));
    }
    return predecessors;
  }

  @Override
  public boolean isDirected() {
    return directed;
  }

  @Override
  public T getNodeById(String id) {
    return index.get(id);
  }

  /**
   * check if adding this edge to the graph creates an cycle
   *
   * @param origin origin of the edge being added
   * @param target target of the edge being added
   * @return true if a cycle is created
   */
  protected boolean checkCycle(T origin, T target) {
    if (origin == target) {
      return true;
    }

    Deque<String> stack = new ArrayDeque<>();

    stack.add(target.getId());
    while (!stack.isEmpty()) {
      String current = stack.pop();
      Set<String> childs = childList.get(current);
      if (childs.contains(origin.getId())) {
        return true;
      }

      for (String child : childs) {
        stack.push(child);
      }
    }

    return false;
  }

  @Override
  public boolean allowsSelfLoops() {
    return allowsSelfLoop;
  }
}
