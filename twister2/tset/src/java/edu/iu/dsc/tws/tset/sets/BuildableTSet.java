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

package edu.iu.dsc.tws.tset.sets;

import java.util.Collection;

import edu.iu.dsc.tws.api.compute.nodes.ICompute;
import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.compute.nodes.ISink;
import edu.iu.dsc.tws.api.compute.nodes.ISource;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.tset.Buildable;

public interface BuildableTSet extends TBase, Buildable {

  int getParallelism();

  INode getINode();

  @Override
  default void build(Collection<? extends TBase> partialBuildSeq) {
    GraphBuilder dfwGraphBuilder = getTBaseGraph().getDfwGraphBuilder();

    if (getINode() instanceof ICompute) {
      dfwGraphBuilder.addTask(getId(), (ICompute) getINode(), getParallelism());
    } else if (getINode() instanceof ISource) {
      dfwGraphBuilder.addSource(getId(), (ISource) getINode(), getParallelism());
    } else if (getINode() instanceof ISink) {
      dfwGraphBuilder.addSink(getId(), (ISink) getINode(), getParallelism());
    } else {
      throw new RuntimeException("Unknown INode " + getINode());
    }
  }

}
