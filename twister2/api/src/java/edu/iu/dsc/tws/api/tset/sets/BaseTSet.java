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

import java.util.Objects;

import com.google.common.reflect.TypeToken;

import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.env.TSetEnvironment;

public abstract class BaseTSet<T> implements BuildableTSet {

  /**
   * The TSet Env to use for runtime operations of the Tset
   */
  private TSetEnvironment tSetEnv;

  /**
   * Name of the data set
   */
  private String name;

  /**
   * ID of the tset
   */
  private String id;

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
    this.id = n;
    this.parallelism = parallel;

    this.name = id;
  }

  protected void rename(String n) {
    this.name = n;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
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

  protected void addChildToGraph(TBase child) {
    tSetEnv.getGraph().addTSet(this, child);
  }

  protected void addChildToGraph(TBase parent, TBase child) {
    tSetEnv.getGraph().addTSet(parent, child);
  }

  @Override
  public String toString() {
    return getName() + "(" + getId() + ")[" + getParallelism() + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BaseTSet<?> baseTSet = (BaseTSet<?>) o;
    return Objects.equals(id, baseTSet.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
