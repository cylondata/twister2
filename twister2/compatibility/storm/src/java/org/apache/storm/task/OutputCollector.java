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
import java.util.Collections;
import java.util.List;

import org.apache.storm.tuple.Tuple;

import edu.iu.dsc.tws.task.api.TaskContext;

public class OutputCollector implements IOutputCollector {

  private final TaskContext t2TaskContext;

  public OutputCollector(TaskContext t2TaskContext) {
    this.t2TaskContext = t2TaskContext;
  }

  @Override
  public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
    t2TaskContext.write(streamId, tuple);//todo anchors?
    return Collections.singletonList(0);
  }

  @Override
  public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {

  }

  @Override
  public void ack(Tuple input) {

  }

  @Override
  public void fail(Tuple input) {

  }

  @Override
  public void reportError(Throwable error) {

  }
}
