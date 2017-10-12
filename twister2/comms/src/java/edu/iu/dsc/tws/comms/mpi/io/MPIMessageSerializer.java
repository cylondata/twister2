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
package edu.iu.dsc.tws.comms.mpi.io;

import java.util.Queue;

import edu.iu.dsc.tws.comms.api.Message;
import edu.iu.dsc.tws.comms.api.MessageSerializer;
import edu.iu.dsc.tws.comms.mpi.MPIBuffer;
import edu.iu.dsc.tws.comms.mpi.MPIMessageReleaseCallback;
import edu.iu.dsc.tws.comms.mpi.MPIMessageType;

public class MPIMessageSerializer implements MessageSerializer {
  private Queue<MPIBuffer> sendBuffers;

  public MPIMessageSerializer(int taskId, MPIMessageType type,
                              MPIMessageReleaseCallback releaseCallback) {
  }

  @Override
  public Object build(Message message) {
    return null;
  }

  @Override
  public Object build(Message message, Object partialBuildObject) {
    return null;
  }
}
