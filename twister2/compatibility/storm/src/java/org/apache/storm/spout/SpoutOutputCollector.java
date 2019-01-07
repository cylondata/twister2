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
package org.apache.storm.spout;

import java.util.Collections;
import java.util.List;

import org.apache.storm.utils.Utils;

import edu.iu.dsc.tws.task.api.TaskContext;

public class SpoutOutputCollector implements ISpoutOutputCollector {

  private final TaskContext taskContext;

  /**
   * @param taskContext the instance of twister2 task context
   */
  public SpoutOutputCollector(TaskContext taskContext) {
    this.taskContext = taskContext;
  }

  /**
   * Emits a tuple to the default output stream with a null message id. Storm will
   * not track this message so ack and fail will never be called for this tuple. The
   * emitted values must be immutable.
   */
  public List<Integer> emit(List<Object> tuple) {
    return emit(tuple, null);
  }

  /**
   * Emits a new tuple to the default output stream with the given message ID.
   * When Storm detects that this tuple has been fully processed, or has failed
   * to be fully processed, the spout will receive an ack or fail callback respectively
   * with the messageId as long as the messageId was not null. If the messageId was null,
   * Storm will not track the tuple and no callback will be received.
   * Note that Storm's event logging functionality will only work if the messageId
   * is serializable via Kryo or the Serializable interface. The emitted values must be immutable.
   *
   * @return the list of task ids that this tuple was sent to
   */
  public List<Integer> emit(List<Object> tuple, Object messageId) {
    return emit(Utils.DEFAULT_STREAM_ID, tuple, messageId);
  }

  @Override
  public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
    this.taskContext.write(streamId, messageId, tuple);
    //todo return task ids
    return Collections.singletonList(0);
  }

  @Override
  public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {

  }

  @Override
  public void reportError(Throwable error) {

  }
}
