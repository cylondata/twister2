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
package edu.iu.dsc.tws.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.executor.comm.IParallelOperation;

public class ExecutionPlan {

  private int numThreads;

  private Map<Integer, INodeInstance> nodes = new HashMap<>();

  private Map<Integer, IParallelOperation> inputMessages = new HashMap<>();

  private Map<Integer, IParallelOperation> outputMessages = new HashMap<>();

  private List<IParallelOperation> parallelOperations = new ArrayList<>();

  /**
   * Add a node instance of this execution
   * @param taskId the global task id
   * @param node the instance
   */
  public void addNodes(int taskId, INodeInstance node) {
    nodes.put(taskId, node);
  }

  public Map<Integer, INodeInstance> getNodes() {
    return nodes;
  }

  public void addOps(IParallelOperation op) {
    parallelOperations.add(op);
  }

  public List<IParallelOperation> getParallelOperations() {
    return parallelOperations;
  }

  public void stop() {

  }

  public int getNumThreads() {
    return numThreads;
  }

  public void setNumThreads(int numThreads) {
    this.numThreads = numThreads;
  }
}
