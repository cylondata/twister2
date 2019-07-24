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

package edu.iu.dsc.tws.api.tset.link.streaming;

import edu.iu.dsc.tws.api.comms.OperationNames;
import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.link.TupleValueIteratorLink;

/**
 * Create a gather data set
 *
 * @param <T> the type of data
 */
public class StreamingGatherTLink<T> extends TupleValueIteratorLink<Integer, T> {

  public StreamingGatherTLink(TSetEnvironment tSetEnv, int sourceParallelism) {
    super(tSetEnv, TSetUtils.generateName("sgather"), sourceParallelism, 1);
  }

  @Override
  public Edge getEdge() {
    return new Edge(getName(), OperationNames.GATHER, getMessageType());
  }

  @Override
  public StreamingGatherTLink<T> setName(String n) {
    rename(n);
    return this;
  }
}
