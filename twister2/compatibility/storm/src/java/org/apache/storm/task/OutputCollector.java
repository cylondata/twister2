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
package org.apache.storm.task;

import java.util.Collection;
import java.util.List;

import org.apache.storm.tuple.Tuple;

public class OutputCollector implements IOutputCollector {

  private final IOutputCollector listener;

  public OutputCollector(IOutputCollector listener) {
    this.listener = listener;
  }

  @Override
  public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
    return this.listener.emit(streamId, anchors, tuple);
  }

  @Override
  public void emitDirect(int taskId,
                         String streamId,
                         Collection<Tuple> anchors,
                         List<Object> tuple) {
    this.listener.emitDirect(taskId, streamId, anchors, tuple);
  }

  @Override
  public void ack(Tuple input) {
    this.listener.ack(input);
  }

  @Override
  public void fail(Tuple input) {
    this.listener.fail(input);
  }

  @Override
  public void reportError(Throwable error) {
    this.listener.reportError(error);
  }
}
