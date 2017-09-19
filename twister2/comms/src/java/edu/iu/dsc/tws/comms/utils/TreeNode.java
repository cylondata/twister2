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

import java.util.ArrayList;
import java.util.List;

public class TreeNode {

  public enum NodeType {
    MIDDLE,
    LEAF
  }

  // children that go through network
  private List<TreeNode> children = new ArrayList<>();

  // in memory children that we should send directly
  private List<Integer> directChildren = new ArrayList<>();

  private TreeNode parent;
  private int taskId;
  private int groupId;

  public TreeNode(int taskId, int groupId) {
    this.taskId = taskId;
    this.groupId = groupId;
  }

  public TreeNode(TreeNode parent, int taskId, int grpId) {
    this.parent = parent;
    this.taskId = taskId;
    this.groupId = grpId;
  }

  public void addChild(TreeNode child) {
    children.add(child);
  }

  public List<TreeNode> getChildren() {
    return children;
  }

  public TreeNode getParent() {
    return parent;
  }

  public int getTaskId() {
    return taskId;
  }

  public int getGroupId() {
    return groupId;
  }

  public void setParent(TreeNode node) {
    this.parent = node;
  }

  public void addChildren(List<TreeNode> nodes) {
    this.children.addAll(nodes);
  }

  public void addDirectChildren(List<Integer> nodes) {
    this.directChildren.addAll(nodes);
  }

  public void addDirectChild(int task) {
    this.directChildren.add(task);
  }
}

