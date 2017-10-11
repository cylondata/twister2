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

import java.io.OutputStream;

import edu.iu.dsc.tws.comms.api.Message;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageSerializer;
import edu.iu.dsc.tws.comms.mpi.MPIMessage;
import edu.iu.dsc.tws.comms.mpi.MPIMessageReleaseCallback;
import edu.iu.dsc.tws.comms.mpi.MPIMessageType;

public class MPIMessageSerializer implements MessageSerializer {
  private int taskId;

  private MPIMessageType type;

  private MPIMessageReleaseCallback releaseCallback;

  public MPIMessageSerializer(int taskId, MPIMessageType type,
                              MPIMessageReleaseCallback releaseCallback) {
    this.taskId = taskId;
    this.type = type;
    this.releaseCallback = releaseCallback;
  }

  @Override
  public Object build(OutputStream stream, Message message) {
    // now create the header
    MessageHeader header = MessageHeader.newBuilder(taskId, 0, 0, 0, 0).build();
    MPIMessage mpiMessage = new MPIMessage(taskId, header, type, releaseCallback);

    return mpiMessage;
  }
}
