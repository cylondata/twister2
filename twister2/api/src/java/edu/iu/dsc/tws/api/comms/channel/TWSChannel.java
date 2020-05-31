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
package edu.iu.dsc.tws.api.comms.channel;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Queue;

import edu.iu.dsc.tws.api.comms.messaging.ChannelMessage;
import edu.iu.dsc.tws.api.comms.packing.DataBuffer;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * Represent a communication channel. A MPI channel or a TCP channel.
 */
public interface TWSChannel {
  /**
   * Send a message
   * @param id worker id
   * @param message message
   * @param callback callback for message completions
   * @return true if sending is accepted
   */
  boolean sendMessage(int id, ChannelMessage message, ChannelListener callback);

  /**
   * Receive a message
   * @param id worker id
   * @param edge the graph edge to receive from
   * @param callback callback for message completions
   * @param receiveBuffers the list of receive buffers
   * @return true if sending is accepted
   */
  boolean receiveMessage(int group, int id, int edge,
                         ChannelListener callback, Queue<DataBuffer> receiveBuffers);

  /**
   * Progress both sends and receives
   */
  void progress();

  /**
   * Progress only the sends
   */
  void progressSends();

  /**
   * Progress only receives belonging to a group
   */
  void progressReceives(int group);

  /**
   * Check weather we have any pending sends
   */
  boolean isComplete();

  /**
   * Create a buffer
   * @param capacity capacity
   * @return the byte buffer
   */
  ByteBuffer createBuffer(int capacity);

  /**
   * Close the channel
   */
  void close();

  /**
   * Release the buffers for worker and edge
   * @param wId worker
   * @param e edge
   */
  void releaseBuffers(int wId, int e);

  /**
   * re-initialize the channel with restarted workers
   * @param restartedWorkers
   */
  void reInit(List<JobMasterAPI.WorkerInfo> restartedWorkers);

}
