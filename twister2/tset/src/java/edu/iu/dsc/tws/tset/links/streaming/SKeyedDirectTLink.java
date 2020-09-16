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

package edu.iu.dsc.tws.tset.links.streaming;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.OperationNames;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.schema.KeyedSchema;
import edu.iu.dsc.tws.tset.env.StreamingEnvironment;
import edu.iu.dsc.tws.tset.links.TLinkUtils;
import edu.iu.dsc.tws.tset.sets.streaming.SKeyedTSet;

public class SKeyedDirectTLink<K, V> extends StreamingSingleLink<Tuple<K, V>> {

  public SKeyedDirectTLink(StreamingEnvironment tSetEnv, int sourceParallelism,
                           KeyedSchema schema) {
    // NOTE: key type is omitted. Check getEdge() method
    super(tSetEnv, "skdirect", sourceParallelism, schema);
  }

  public SKeyedTSet<K, V> mapToTuple() {
    return super.mapToTuple((MapFunc<Tuple<K, V>, Tuple<K, V>>) input -> input);
  }

  @Override
  public Edge getEdge() {
    // NOTE: There is no keyed direct in the communication layer!!! Hence this is an keyed direct
    // emulation. Therefore, we can not use user provided data types here because we will be using
    // Tuple<K, V> object through a DirectLink here.
    // todo fix this ambiguity!
    Edge e = new Edge(getId(), OperationNames.DIRECT, MessageTypes.OBJECT);
    TLinkUtils.generateCommsSchema(getSchema(), e);
    return e;
  }

  @Override
  public SKeyedDirectTLink<K, V> setName(String n) {
    rename(n);
    return this;
  }
}
