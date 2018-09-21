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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.proto.checkpoint.Checkpoint;

public class TaskBarrierMonitor implements MessageHandler {

  private static final Logger LOG = Logger.getLogger(TaskBarrierMonitor.class.getName());

  private CheckpointManager checkpointManager;
  private Config config;
  private RRServer rrServer;

  private List<Integer> sourceTaskList;
  private List<Integer> sinkTaskList;

  private int sourceParallelism;
  private int sinkParallelism;

  private Set<Integer> currentBarrierReceivedSourceSet;

  private boolean sendBarrierFlag;
  private int currentBarrierID;

  private boolean allTaskGotRegistered;

  public TaskBarrierMonitor(Config cfg, CheckpointManager checkpointManager, RRServer server) {
    this.config = cfg;
    this.checkpointManager = checkpointManager;
    this.rrServer = server;
    this.sourceTaskList = new ArrayList<>();
    this.sinkTaskList = new ArrayList<>();

    this.sourceParallelism = 0;
    this.sinkParallelism = 0;

    this.allTaskGotRegistered = false;
  }

  @Override
  public void onMessage(RequestID id, int taskId, Message message) {

    if (message instanceof Checkpoint.TaskDiscovery) {
      Checkpoint.TaskDiscovery taskDiscoveryMessage = (Checkpoint.TaskDiscovery) message;

      if (taskDiscoveryMessage.getTaskType().equals(Checkpoint.TaskDiscovery.TaskType.SOURCE)) {
        LOG.fine("Source task with ID " + taskDiscoveryMessage.getTaskID()
            + " registered with Checkpoint Manager");

        this.sourceTaskList.add(taskDiscoveryMessage.getTaskID());

        if (this.sourceParallelism == 0) {
          sourceParallelism = taskDiscoveryMessage.getParrallelism();
        }

        checkAllTaskGotRegistered();


      } else if (taskDiscoveryMessage.getTaskType()
          .equals(Checkpoint.TaskDiscovery.TaskType.SINK)) {

        LOG.fine("Sink task with ID " + taskDiscoveryMessage.getTaskID()
            + " registered with Checkpoint Manager");

        this.sinkTaskList.add(taskDiscoveryMessage.getTaskID());

        if (this.sinkParallelism == 0) {
          sinkParallelism = taskDiscoveryMessage.getParrallelism();
        }

        checkAllTaskGotRegistered();

      }

    } else if (message instanceof Checkpoint.BarrierSync) {

      LOG.fine("Source task " + taskId + " sent BarrierSync message.");
      Checkpoint.BarrierSync barrierSyncMessage = (Checkpoint.BarrierSync) message;

      if (currentBarrierID == barrierSyncMessage.getCurrentBarrierID() && sendBarrierFlag
          && allTaskGotRegistered) {

        Checkpoint.BarrierSend barrierSendMessage = Checkpoint.BarrierSend.newBuilder()
            .setSendBarrier(true)
            .setCurrentBarrierID(currentBarrierID)
            .build();

        rrServer.sendResponse(id, barrierSendMessage);

        currentBarrierReceivedSourceSet.add(barrierSyncMessage.getTaskID());

        checkAllBarrierGotSent();

      } else {

        Checkpoint.BarrierSend barrierSendMessage = Checkpoint.BarrierSend.newBuilder()
            .setSendBarrier(false)
            .setCurrentBarrierID(currentBarrierID)
            .build();

        rrServer.sendResponse(id, barrierSendMessage);
      }

    }
  }

  private void printTaskList(List<Integer> ids, String type) {
    String temp = type + " Task IDs";
    for (Integer i : ids) {
      temp += " " + i;
    }
    LOG.info(temp);
  }

  /**
   * This will check whether all the source and sink tasks have got registered
   */
  private void checkAllTaskGotRegistered() {
    if ((sourceTaskList.size() == sourceParallelism) && (sinkTaskList.size() == sinkParallelism)) {
      printTaskList(sourceTaskList, "Source");
      printTaskList(sinkTaskList, "Sink");

      this.allTaskGotRegistered = true;

      sendBarrierFlag = true;
      currentBarrierID = 1;

      currentBarrierReceivedSourceSet = new HashSet<Integer>();

      LOG.info("All source and sink tasks got registered");
    }

  }

  /**
   * This will make sendBarrier Conditions false so that it will not
   * emit any more barriers until previous barrier got received from the sink
   */
  private void checkAllBarrierGotSent() {
    if (currentBarrierReceivedSourceSet.size() == sourceParallelism) {
      currentBarrierReceivedSourceSet = new HashSet<Integer>();
      sendBarrierFlag = false;

      LOG.info("Barriers with Barrier ID " + currentBarrierID + " got sent from Source Tasks");
    }
  }
}
