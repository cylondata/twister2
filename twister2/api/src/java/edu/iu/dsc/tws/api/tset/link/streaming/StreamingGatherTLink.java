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

import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.link.BaseTLink;
import edu.iu.dsc.tws.executor.core.OperationNames;

/**
 * Create a gather data set
 *
 * @param <T> the type of data
 */
public class StreamingGatherTLink<T> extends BaseTLink<T> {

  public StreamingGatherTLink(TSetEnvironment tSetEnv, int sourceParallelism) {
    super(tSetEnv, TSetUtils.generateName("sgather"), sourceParallelism, 1);
  }

/*  public <P> StreamingMapTSet<T, P> map(MapFunction<T, P> mapFn) {
    StreamingMapTSet<T, P> set = new StreamingMapTSet<>(getTSetEnv(), mapFn, 1);
    addChildToGraph(set);
    return set;
  }

  public <P> StreamingFlatMapTSet<T, P> flatMap(FlatMapFunction<T, P> mapFn) {
    StreamingFlatMapTSet<T, P> set = new StreamingFlatMapTSet<>(getTSetEnv(), mapFn, 1);
    addChildToGraph(set);
    return set;
  }*/

/*
  @Override
  public void build(TSetGraph tSetGraph) {
//    MessageType dataType = TSetUtils.getDataType(getType());
//
//    connection.gather(parent.getName()).viaEdge(Constants.DEFAULT_EDGE).withDataType(dataType);
  }
*/

  @Override
  protected Edge getEdge() {
    return new Edge(getName(), OperationNames.GATHER, getMessageType());
  }

  @Override
  public StreamingGatherTLink<T> setName(String n) {
    rename(n);
    return this;
  }
}
