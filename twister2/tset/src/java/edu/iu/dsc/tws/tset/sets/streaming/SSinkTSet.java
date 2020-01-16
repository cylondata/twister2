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


package edu.iu.dsc.tws.tset.sets.streaming;

import java.util.Collections;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.compute.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.tset.env.StreamingTSetEnvironment;
import edu.iu.dsc.tws.tset.ops.SinkOp;

public class SSinkTSet<T> extends StreamingTSetImpl<T> {
  private SinkFunc<T> sink;

  /**
   * Creates SinkTSet with the given parameters, the parallelism of the TSet is taken as 1
   *
   * @param tSetEnv The TSetEnv used for execution
   * @param s       The Sink function to be used
   */
  public SSinkTSet(StreamingTSetEnvironment tSetEnv, SinkFunc<T> s) {
    this(tSetEnv, s, 1);
  }

  /**
   * Creates SinkTSet with the given parameters
   *
   * @param tSetEnv     The TSetEnv used for execution
   * @param s           The Sink function to be used
   * @param parallelism the parallelism of the sink
   */
  public SSinkTSet(StreamingTSetEnvironment tSetEnv, SinkFunc<T> s, int parallelism) {
    super(tSetEnv, "ssink", parallelism);
    this.sink = s;
  }

  @Override
  public SSinkTSet<T> setName(String n) {
    rename(n);
    return this;
  }

  public SSinkTSet<T> withDataType(MessageType dataType) {
    return (SSinkTSet<T>) super.withDataType(dataType);
  }

  @Override
  public ICompute getINode() {
    return new SinkOp<>(sink, this, Collections.emptyMap());
  }
}
