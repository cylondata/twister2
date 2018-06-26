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
package edu.iu.dsc.tws.checkpointmanager;

import java.util.List;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.proto.checkpoint.Checkpoint;

public class TaskMonitor implements MessageHandler {

  private static final Logger LOG = Logger.getLogger(TaskMonitor.class.getName());

  private CheckpointManager checkpointManager;
  private Config config;
  private RRServer rrServer;

  private List<Integer> sourceTaskList;
  private List<Integer> sinkTaskList;

  public TaskMonitor(Config cfg, CheckpointManager checkpointManager, RRServer server) {
    this.config = cfg;
    this.checkpointManager = checkpointManager;
    this.rrServer = server;
  }

  @Override
  public void onMessage(RequestID id, int taskId, Message message) {

    if (message instanceof Checkpoint.TaskDiscovery) {
      Checkpoint.TaskDiscovery taskDiscoveryMessage = (Checkpoint.TaskDiscovery) message;

      if (taskDiscoveryMessage.getTaskType().equals(Checkpoint.TaskDiscovery.TaskType.SOURCE)) {
        LOG.info("Source task with ID " + taskDiscoveryMessage.getTaskID()
            + " registered with Checkpoint Manager");

        this.sourceTaskList.add(taskDiscoveryMessage.getTaskID());

      } else if (taskDiscoveryMessage.getTaskType()
          .equals(Checkpoint.TaskDiscovery.TaskType.SINK)) {

        LOG.info("Sink task with ID " + taskDiscoveryMessage.getTaskID()
            + " registered with Checkpoint Manager");

        this.sinkTaskList.add(taskDiscoveryMessage.getTaskID());

      }

    }
  }
}
