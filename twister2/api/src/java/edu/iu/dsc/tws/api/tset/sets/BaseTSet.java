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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.reflect.TypeToken;

import edu.iu.dsc.tws.api.tset.Cacheable;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.TSet;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.link.BaseTLink;
import edu.iu.dsc.tws.common.config.Config;

public abstract class BaseTSet<T> implements TSet<T> {
  private static final Logger LOG = Logger.getLogger(BaseTSet.class.getName());
  /**
   * The children of this set
   */
  protected List<TBase<?>> children;

  /**
   * The TSet Env to use for runtime operations of the Tset
   */
  protected TSetEnv tSetEnv;

  /**
   * Name of the data set
   */
  protected String name;

  /**
   * The parallelism of the set
   */
  protected int parallel = 1;
  /**
   * Defines if the TSet is Mutable or not
   */
  private boolean isMutable = false;
  /**
   * The configuration
   */
  protected Config config;

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

  public BaseTSet(Config cfg, TSetEnv tSetEnv) {
    this.children = new ArrayList<>();
    this.tSetEnv = tSetEnv;
    this.config = cfg;
  }

  public String getName() {
    return name;
  }

  public int getParallelism() {
    return parallel;
  }

  @Override
  public BaseTSet<T> setName(String n) {
    this.name = n;
    return this;
  }


  @Override
  public void build() {
    // first build our selves
    baseBuild();

    // then build children
    for (TBase<?> c : children) {
      c.build();
    }
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

  /**
   * Override the parallelism
   *
   * @return if overide, return value, otherwise -1
   */
  protected int overrideParallelism() {
    return -1;
  }

  /**
   * Override the parallelism if operations require differently
   *
   * @return new parallelism
   */
  protected <K> int calculateParallelism(BaseTLink<K> parent) {
    int p;
    if (parent.overrideParallelism() != -1) {
      p = parent.overrideParallelism();
      LOG.log(Level.WARNING, String.format("Overriding parallelism "
          + "specified %d override value %d", parallel, p));
    } else {
      p = parallel;
    }
    return p;
  }

  protected String generateName(String prefix, BaseTLink parent) {
    if (name != null) {
      return name;
    } else {
      if (parent == null) {
        return prefix + "-" + new Random().nextInt(100);
      } else {
        return prefix + "-" + parent.getName();
      }
    }
  }

  @Override
  public boolean addInput(String key, Cacheable<?> input) {
    tSetEnv.addInput(getName(), key, input);
    return true;
  }
}
