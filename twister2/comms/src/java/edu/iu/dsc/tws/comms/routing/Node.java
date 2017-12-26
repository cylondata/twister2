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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Node {
  // children that are not in memory
  private List<Node> children = new ArrayList<>();

  // in memory children
  private List<Integer> directChildren = new ArrayList<>();

  private Node parent;
  private int taskId;
  private int groupId;
  private int groupLevel;
  private int execLevel;

  public Node(int taskId, int groupId) {
    this.taskId = taskId;
    this.groupId = groupId;
  }

  public Node(Node parent, int taskId, int grpId) {
    this.parent = parent;
    this.taskId = taskId;
    this.groupId = grpId;
  }

  public void addChild(Node child) {
    children.add(child);
  }

  public List<Node> getChildren() {
    return children;
  }

  public Node getParent() {
    return parent;
  }

  public Set<Integer> getAllChildrenIds() {
    Set<Integer> allChildren = new HashSet<>();
    allChildren.addAll(directChildren);
    for (Node n : children) {
      allChildren.add(n.getTaskId());
    }
    return allChildren;
  }

  public Set<Integer> getRemoteChildrenIds() {
    Set<Integer> allChildren = new HashSet<>();
    for (Node n : children) {
      allChildren.add(n.getTaskId());
    }
    return allChildren;
  }

  public int getTaskId() {
    return taskId;
  }

  public int getGroupId() {
    return groupId;
  }

  public void setParent(Node node) {
    this.parent = node;
  }

  public void addChildren(List<Node> nodes) {
    this.children.addAll(nodes);
  }

  public void addDirectChildren(List<Integer> nodes) {
    this.directChildren.addAll(nodes);
  }

  public void addDirectChild(int task) {
    this.directChildren.add(task);
  }

  public List<Integer> getDirectChildren() {
    return directChildren;
  }

  public int getGroupLevel() {
    return groupLevel;
  }

  public int getExecLevel() {
    return execLevel;
  }

  public void setGroupLevel(int groupLevel) {
    this.groupLevel = groupLevel;
  }

  public void setExecLevel(int execLevel) {
    this.execLevel = execLevel;
  }

  @Override
  public String toString() {
    return "Node{"
        + "children=" + children
        + ", directChildren=" + directChildren
        + ", parent=" + (parent != null ? parent.getTaskId() + "" : "NULL")
        + ", taskId=" + taskId
        + ", groupId=" + groupId
        + '}';
  }
}

