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

import java.util.Comparator;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.task.graph.Edge;

public class JoinConfig extends AbstractKeyedOpsConfig<JoinConfig> {
  private boolean useDsk;

  private Comparator keyCompartor;

  public JoinConfig(String parent,
                           ComputeConnection computeConnection) {
    super(parent, OperationNames.JOIN, computeConnection);
  }

  public JoinConfig useDisk(boolean useDisk) {
    this.useDsk = useDisk;
    return this.withProperty("use-disk", useDisk);
  }

  public <T> JoinConfig withComparator(Comparator<T> keyComparator) {
    this.keyCompartor = keyComparator;
    return this.withProperty("key-comparator", keyComparator);
  }

  @Override
  void validate() {
    if (this.keyCompartor == null) {
      failValidation("Join operation needs a key comparator.");
    }
  }

  @Override
  protected Edge updateEdge(Edge newEdge) {
    return newEdge;
  }
}
