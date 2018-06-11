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

import java.util.logging.Logger;

import edu.iu.dsc.tws.checkpointmanager.state_backend.StateBackend;
import edu.iu.dsc.tws.master.JobMaster;
import edu.iu.dsc.tws.task.graph.Vertex;

public class CheckpointManager {
  private static final Logger LOG = Logger.getLogger(CheckpointManager.class.getName());

//  private final CheckpointProperties checkpointProperties;

  //TODO : Make the variables final once finalised
  private String jobName;

  private JobMaster jobMaster;

  private long baseInterval;

  private long checkpointTimeout;

  private long minPauseBetweenCheckpoints;

  private long maxConcurrentCheckpointAttempts;

  private volatile boolean shutdown;

  public CheckpointManager(
      String jobName,
      long baseInterval,
      long checkpointTimeout,
      long minPauseBetweenCheckpoints,
      int maxConcurrentCheckpointAttempts,
      Vertex[] tasksToTrigger,
      Vertex[] tasksToWaitFor,
      Vertex[] tasksToCommitTo,
      CheckpointIdCounter checkpointIdCounter,
      CompletedCheckpointStore completedCheckpointStore,
      StateBackend checkpointStateBackend
  ) {

    this.baseInterval = baseInterval;
    this.checkpointTimeout = checkpointTimeout;
    this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
    this.maxConcurrentCheckpointAttempts = maxConcurrentCheckpointAttempts;

  }

  public CheckpointManager(String jobName, JobMaster jobMaster) {
    this.jobName = jobName;
    this.jobMaster = jobMaster;
  }

  public void startCheckpointScheduler() {

  }

  public void stopCheckpointScheduler() {

  }

  public void receiveAcknowledgeMessage() {

  }

  public long getBaseInterval() {
    return baseInterval;
  }

  public long getCheckpointTimeout() {
    return checkpointTimeout;
  }

  public long getMinPauseBetweenCheckpoints() {
    return minPauseBetweenCheckpoints;
  }

  public long getMaxConcurrentCheckpointAttempts() {
    return maxConcurrentCheckpointAttempts;
  }
}
