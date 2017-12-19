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
package edu.iu.dsc.tws.task.taskgraphbuilder;

/**
 * The generic task edge factory for task vertices and task edges.
 */
public interface IDataflowTaskEdgeFactory<TV, TE> {

  /**
   * This method creates a task edge and their endpoints are source task and target task vertices.
   */
  TE createTaskEdge(TV sourceTaskVertex, TV targetTaskVertex)
      throws IllegalAccessException, InstantiationException;
}
