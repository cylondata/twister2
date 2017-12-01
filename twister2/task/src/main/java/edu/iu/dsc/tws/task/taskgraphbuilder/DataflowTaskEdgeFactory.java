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
package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.io.Serializable;

public class DataflowTaskEdgeFactory<TV, TE> implements TaskEdgeFactory<TV, TE>, Serializable {

  private static final long serialVersionUID = 2233333333444448888L;

  public Class<? extends TE> taskEdgeClass;

  public DataflowTaskEdgeFactory(Class<? extends TE> taskEdgeClass) {
    this.taskEdgeClass = taskEdgeClass;
  }

  @Override
  public TE createTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) throws IllegalAccessException {
    System.out.println("Source Task Vertex is:" + sourceTaskVertex);
    try {
      return taskEdgeClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("instance creation failed", e);
    }
  }
}

