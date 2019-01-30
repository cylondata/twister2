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
package org.apache.storm.topology.twister2;

import java.util.HashMap;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

public class EdgeFieldMap extends HashMap<String, Fields> implements OutputFieldsDeclarer {

  private String defaultEdge;

  public EdgeFieldMap(String defaultEdge) {
    this.defaultEdge = defaultEdge;
  }

  @Override
  public void declare(Fields fields) {
    this.declareStream(this.defaultEdge, fields);
  }

  @Override
  public void declare(boolean direct, Fields fields) {
    //todo
    throw new UnsupportedOperationException("Direct output field declaration "
        + "is not supported yet");
  }

  @Override
  public void declareStream(String streamId, Fields fields) {
    this.put(
        streamId,
        fields
    );
  }

  @Override
  public void declareStream(String streamId, boolean direct, Fields fields) {
    //todo
    throw new UnsupportedOperationException("Direct output field declaration "
        + "is not supported yet");
  }
}
