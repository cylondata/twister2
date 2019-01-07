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

import java.util.Collections;
import java.util.List;

import org.apache.storm.utils.Utils;

import edu.iu.dsc.tws.task.api.TaskContext;

public class BasicOutputCollector implements IBasicOutputCollector {

  private final TaskContext t2TaskContext;

  public BasicOutputCollector(TaskContext t2TaskContext) {
    this.t2TaskContext = t2TaskContext;
  }

  public List<Integer> emit(List<Object> tuple) {
    return emit(Utils.DEFAULT_STREAM_ID, tuple);
  }

  @Override
  public List<Integer> emit(String streamId, List<Object> tuple) {
    this.t2TaskContext.write(streamId, tuple);
    //todo return task ids
    return Collections.singletonList(0);
  }

  @Override
  public void emitDirect(int taskId, String streamId, List<Object> tuple) {

  }

  @Override
  public void reportError(Throwable t) {

  }
}
