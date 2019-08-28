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

package edu.iu.dsc.tws.tset.links.batch;

import edu.iu.dsc.tws.api.compute.OperationNames;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;

public class PartitionTLink<T> extends BIteratorLink<T> {

  private PartitionFunc<T> partitionFunction;

  public PartitionTLink(BatchTSetEnvironment tSetEnv, int sourceParallelism) {
    this(tSetEnv, null, sourceParallelism);
  }

  public PartitionTLink(BatchTSetEnvironment tSetEnv, PartitionFunc<T> parFn,
                        int sourceParallelism) {
    this(tSetEnv, parFn, sourceParallelism, sourceParallelism);
  }

  public PartitionTLink(BatchTSetEnvironment tSetEnv, PartitionFunc<T> parFn,
                        int sourceParallelism, int targetParallelism) {
    super(tSetEnv, TSetUtils.generateName("partition"),
        sourceParallelism, targetParallelism);
    this.partitionFunction = parFn;
  }


  @Override
  public Edge getEdge() {
    Edge e = new Edge(getId(), OperationNames.PARTITION, getMessageType());
    if (partitionFunction != null) {
      e.setPartitioner(partitionFunction);
    }
    return e;
  }

  @Override
  public PartitionTLink<T> setName(String n) {
    rename(n);
    return this;
  }
}
