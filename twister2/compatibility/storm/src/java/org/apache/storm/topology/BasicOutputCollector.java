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
package org.apache.storm.topology;

import java.util.List;

import org.apache.storm.task.OutputCollector;

public class BasicOutputCollector implements IBasicOutputCollector {

  private final String defaultEdge;
  private OutputCollector outputCollector;

  public BasicOutputCollector(OutputCollector outputCollector, String defaultEdge) {
    this.outputCollector = outputCollector;
    this.defaultEdge = defaultEdge;
  }

  public List<Integer> emit(List<Object> tuple) {
    return emit(this.defaultEdge, tuple);
  }

  @Override
  public List<Integer> emit(String streamId, List<Object> tuple) {
    return this.outputCollector.emit(streamId, (List) null, tuple);
  }

  @Override
  public void emitDirect(int taskId, String streamId, List<Object> tuple) {
    this.outputCollector.emitDirect(taskId, streamId, null, tuple);
  }

  @Override
  public void reportError(Throwable t) {
    this.outputCollector.reportError(t);
  }
}
