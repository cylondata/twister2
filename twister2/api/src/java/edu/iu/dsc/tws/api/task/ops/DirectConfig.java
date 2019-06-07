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
package edu.iu.dsc.tws.api.task.ops;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.task.graph.Edge;

public class DirectConfig extends AbstractOpsConfig<DirectConfig> {

  protected DirectConfig(String source, ComputeConnection computeConnection) {
    super(source, OperationNames.DIRECT, computeConnection);
  }

  @Override
  void validate() {
    //nothing to do here
  }

  @Override
  protected Edge updateEdge(Edge newEdge) {
    return newEdge;
  }
}
