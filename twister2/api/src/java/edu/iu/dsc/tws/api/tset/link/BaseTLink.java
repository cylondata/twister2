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
package edu.iu.dsc.tws.api.tset.link;

import java.util.Set;

import com.google.common.reflect.TypeToken;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetGraph;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.Sink;
import edu.iu.dsc.tws.api.tset.sets.SinkTSet;

public abstract class BaseTLink<T> implements TLink<T> {

  /**
   * The TSet Env used for runtime operations
   */
  private TSetEnvironment tSetEnv;

  /**
   * Name of the data set
   */
  private String name;

  private int sourceParallelism;

  private int targetParallelism;

  private TBase<?> source;

  private TBase<?> target;

  public BaseTLink(TSetEnvironment env, String n) {
    this(env, n, env.getDefaultParallelism());
  }

  public BaseTLink(TSetEnvironment env, String n, int sourceP) {
    this(env, n, sourceP, sourceP);
  }

  public BaseTLink(TSetEnvironment env, String n, int sourceP, int targetP) {
    this.tSetEnv = env;
    this.name = n;
    this.sourceParallelism = sourceP;
    this.targetParallelism = targetP;
  }


  public SinkTSet<T> sink(Sink<T> sink) {
    SinkTSet<T> sinkTSet = new SinkTSet<>(tSetEnv, sink, targetParallelism);
    addChildToGraph(sinkTSet);
    tSetEnv.executeSink(sinkTSet);
    return sinkTSet;
  }


  public String getName() {
    return name;
  }

  protected void rename(String n) {
    this.name = n;
  }

  protected Class getType() {
    TypeToken<T> typeToken = new TypeToken<T>(getClass()) {
    };
    return typeToken.getRawType();
  }

  protected MessageType getMessageType() {
    return TSetUtils.getDataType(getType());
  }

  protected void addChildToGraph(TBase child) {
    tSetEnv.getGraph().addTSet(this, child);
  }

  public TSetEnvironment getTSetEnv() {
    return tSetEnv;
  }

  public int getSourceParallelism() {
    return sourceParallelism;
  }

  public int getTargetParallelism() {
    return targetParallelism;
  }

  TBase<?> getSource() {
    return this.source;
  }

  TBase<?> getTarget() {
    return this.target;
  }

  private void validateTlink(TSetGraph graph) {
    Set<TBase> parents = graph.getPredecessors(this);
    if (parents.size() != 1) {
      throw new RuntimeException("tlinks must have only one source!");
    }
    this.source = parents.iterator().next();

    Set<TBase> children = graph.getSuccessors(this);
    if (children.size() != 1) {
      throw new RuntimeException("tlinks must have only one target!");
    }

    this.target = children.iterator().next();
  }

  @Override
  public void build(TSetGraph tSetGraph) {
    validateTlink(tSetGraph);
    tSetGraph.getDfwGraphBuilder().connect(getSource().getName(), getTarget().getName(), getEdge());
  }

  protected abstract Edge getEdge();

  @Override
  public String toString() {
    return "[Tlink:" + getName() + "]";
  }
}
