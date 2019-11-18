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

/**
 * DAG graph implementation
 *
 * @param <T> node type
 */
public class DAGMutableGraph<T> implements MutableGraph<T> {

  private final boolean directed;
  private Map<T, Set<T>> childList;
  private Map<T, Set<T>> parentList;
  private boolean allowsSelfLoop;

  public DAGMutableGraph() {
    this.directed = true;
    this.allowsSelfLoop = false;
    childList = new HashMap<>();
    parentList = new HashMap<>();
  }

  @Override
  public boolean addNode(T node) {
    if (childList.containsKey(node)) {
      return false;
    }

    childList.put(node, new HashSet<>());
    parentList.put(node, new HashSet<>());
    return true;
  }

  @Override
  public boolean putEdge(T orgin, T target) {

    if (checkCycle(orgin, target)) {
      throw new IllegalStateException("This edge introduces an cycle to this graph");
    }
    addNode(orgin);
    addNode(target);
    if (childList.get(orgin).contains(target)) {
      return false;
    }

    childList.get(orgin).add(target);
    parentList.get(target).add(orgin);
    return true;
  }

  @Override
  public Set<T> nodes() {
    return childList.keySet();
  }

  @Override
  public boolean removeNode(T node) {
    if (!childList.containsKey(node)) {
      return false;
    }

    // remove the edges to the node from its parents
    Set<T> parents = parentList.remove(node);
    for (T parent : parents) {
      childList.get(parent).remove(node);
    }

    // remove this node from the parent list form all the child nodes
    Set<T> childs = childList.remove(node);
    for (T child : childs) {
      parentList.get(child).remove(node);
    }

    return true;
  }

  @Override
  public boolean removeEdge(T origin, T target) {
    if (!childList.get(origin).contains(target)) {
      return false;
    }

    childList.get(origin).remove(target);
    parentList.get(target).remove(origin);
    return true;
  }

  @Override
  public Set<T> successors(T node) {
    return childList.get(node);
  }

  @Override
  public Set<T> predecessors(T node) {
    return parentList.get(node);
  }

  @Override
  public boolean isDirected() {
    return directed;
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

    Deque<T> stack = new ArrayDeque<>();

    stack.add(target);
    while (!stack.isEmpty()) {
      T current = stack.pop();
      Set<T> childs = childList.get(current);
      if (childs.contains(origin)) {
        return true;
      }

      for (T child : childs) {
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
