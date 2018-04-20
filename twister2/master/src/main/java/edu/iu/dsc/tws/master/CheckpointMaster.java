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
package edu.iu.dsc.tws.master;

import java.util.logging.Logger;

import edu.iu.dsc.tws.executor.IExecutor;

public class CheckpointMaster {
  private static final Logger LOG = Logger.getLogger(CheckpointMaster.class.getName());

  private final IExecutor executor;

//  private final CheckpointProperties checkpointProperties;

  private final long baseInterval;

  private final long checkpointTimeout;

  private final long minPauseBetweenCheckpoints;

  private final long maxConcurrentCheckpointAttempts;

  private volatile boolean shutdown;

  public CheckpointMaster(
      long baseInterval,
      long checkpointTimeout,
      long minPauseBetweenCheckpoints,
      int maxConcurrentCheckpointAttempts,
      IExecutor executor
  ){

    this.baseInterval = baseInterval;
    this.checkpointTimeout = checkpointTimeout;
    this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
    this.maxConcurrentCheckpointAttempts = maxConcurrentCheckpointAttempts;
    this.executor = executor;

  }

  public void startCheckpointScheduler(){

  }

  public void stopCheckpointScheduler(){

  }

  public void receiveAcknowledgeMessage(){

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
