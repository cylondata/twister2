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

package edu.iu.dsc.tws.tset.links;

import java.util.Objects;

import com.google.common.reflect.TypeToken;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.tset.TSetUtils;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;

/**
 * Base link impl for all the links
 *
 * @param <T1> output type from the comms
 * @param <T0> base type
 */
public abstract class BaseTLink<T1, T0> implements BuildableTLink {

  /**
   * The TSet Env used for runtime operations
   */
  private TSetEnvironment tSetEnv;

  /**
   * Name of the data set
   */
  private String name;

  /**
   * ID of the tlink
   */
  private String thisID;

  private int sourceParallelism;

  private int targetParallelism;

  public BaseTLink(TSetEnvironment env, String id) {
    this(env, id, env.getDefaultParallelism());
  }

  public BaseTLink(TSetEnvironment env, String id, int sourceP) {
    this(env, id, sourceP, sourceP);
  }

  public BaseTLink(TSetEnvironment env, String id, int sourceP, int targetP) {
    this.tSetEnv = env;
    this.thisID = generateID(id);
    this.sourceParallelism = sourceP;
    this.targetParallelism = targetP;

    this.name = id;
  }

  protected BaseTLink() {
  }

  @Override
  public String getId() {
    return thisID;
  }

  @Override
  public String getName() {
    return name;
  }

  protected void rename(String n) {
    this.name = n;
  }

  protected Class getType() {
    TypeToken<T1> typeToken = new TypeToken<T1>(getClass()) {
    };
    return typeToken.getRawType();
  }

  //todo: this always return Object type!!!
  protected MessageType getMessageType() {
    return TSetUtils.getDataType(getType());
  }

  protected void addChildToGraph(TBase child) {
    tSetEnv.getGraph().addTSet(this, child);
  }

  public TSetEnvironment getTSetEnv() {
    return tSetEnv;
  }

  public void setTSetEnv(TSetEnvironment newEnv) {
    this.tSetEnv = newEnv;
  }

  public int getSourceParallelism() {
    return sourceParallelism;
  }

  public int getTargetParallelism() {
    return targetParallelism;
  }

  @Override
  public String toString() {
    return getName() + "{"
        + tSetEnv.getGraph().getPredecessors(this)
        + "->" + tSetEnv.getGraph().getSuccessors(this)
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BaseTLink<?, ?> baseTLink = (BaseTLink<?, ?>) o;
    return thisID.equals(baseTLink.thisID);
  }

  @Override
  public int hashCode() {
    return Objects.hash(thisID);
  }
}
