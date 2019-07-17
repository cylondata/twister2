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

package edu.iu.dsc.tws.api.tset.sets;

import com.google.common.reflect.TypeToken;

import edu.iu.dsc.tws.api.task.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.Cacheable;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;

public abstract class BaseTSet<T> implements TSet<T> {

  /**
   * The TSet Env to use for runtime operations of the Tset
   */
  private TSetEnvironment tSetEnv;

  /**
   * Name of the data set
   */
  private String name;

  /**
   * The parallelism of the set
   */
  private int parallelism;

  /**
   * Defines if the TSet is Mutable or not
   */
  private boolean isMutable = false;

  /**
   * Possible Types of state in a TSet
   */
  public enum StateType {
    /**
     * Local state which is updated and maintained by each parallel task
     */
    LOCAL,
    /**
     * Distributed state is when each task has only access to a subset of the whole data
     * for example if the data set has N points and T tasks each task will access N/T points
     */
    DISTRIBUTED,
    /**
     * Replicated state is state that is made available as a whole to each task
     */
    REPLICATED
  }

  /**
   * The type of the TSet
   */
  private StateType stateType = StateType.DISTRIBUTED;

  public BaseTSet(TSetEnvironment tSetEnv, String name) {
    this(tSetEnv, name, tSetEnv.getDefaultParallelism());
  }

  public BaseTSet(TSetEnvironment env, String n, int parallel) {
    this.tSetEnv = env;
    this.name = n;
    this.parallelism = parallel;
  }

  protected void rename(String n) {
    this.name = n;
  }

  @Override
  public String getName() {
    return name;
  }

  public int getParallelism() {
    return parallelism;
  }

  public TSetEnvironment getTSetEnv() {
    return tSetEnv;
  }

  public boolean isMutable() {
    return isMutable;
  }

  public void setMutable(boolean mutable) {
    isMutable = mutable;
  }

  public StateType getStateType() {
    return stateType;
  }

  public void setStateType(StateType stateType) {
    this.stateType = stateType;
  }

  protected Class getType() {
    TypeToken<T> typeToken = new TypeToken<T>(getClass()) {
    };
    return typeToken.getRawType();
  }

  @Override
  public boolean addInput(String key, Cacheable<?> input) {
    tSetEnv.addInput(getName(), key, input);
    return true;
  }

  protected void addChildToGraph(TBase child) {
    tSetEnv.getGraph().addTSet(this, child);
  }

  @Override
  public void build(TSetGraph tSetGraph) {
    GraphBuilder dfwGraphBuilder = tSetGraph.getDfwGraphBuilder();

    // add task to the graph
    dfwGraphBuilder.addTask(getName(), getTask(), getParallelism());
  }

  protected abstract ICompute getTask();

  @Override
  public String toString() {
    return "[Tset:" + getName() + "]";
  }
}
