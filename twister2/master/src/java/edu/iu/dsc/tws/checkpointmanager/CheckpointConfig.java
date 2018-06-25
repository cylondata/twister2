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

import static java.util.Objects.requireNonNull;

public class CheckpointConfig {
  private static final Logger LOG = Logger.getLogger(CheckpointConfig.class.getName());

  public static final CheckpointingMode DEFAULT_MODE = CheckpointingMode.EXACTLY_ONCE;

  public static final long DEFAULT_TIMEOUT = 10 * 60 * 1000;

  public static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS = 0;

  public static final int DEFAULT_MAX_CONCURRENT_CHECKPOINTS = 1;


  private CheckpointingMode checkpointingMode = DEFAULT_MODE;

  private long checkpointInterval = -1; // disabled

  private long checkpointTimeout = DEFAULT_TIMEOUT;

  private long minPauseBetweenCheckpoints = DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS;

  private int maxConcurrentCheckpoints = DEFAULT_MAX_CONCURRENT_CHECKPOINTS;

  private boolean forceCheckpointing;

  public boolean isCheckpointingEnabled() {
    return checkpointInterval > 0;
  }

  public CheckpointingMode getCheckpointingMode() {
    return checkpointingMode;
  }

  public void setCheckpointingMode(CheckpointingMode checkpointingMode) {
    this.checkpointingMode = requireNonNull(checkpointingMode);
  }

  public long getCheckpointInterval() {
    return checkpointInterval;
  }


  public void setCheckpointInterval(long checkpointInterval) {
    if (checkpointInterval <= 0) {
      throw new IllegalArgumentException("Checkpoint interval must be larger than zero");
    }
    this.checkpointInterval = checkpointInterval;
  }

  public long getCheckpointTimeout() {
    return checkpointTimeout;
  }

  public void setCheckpointTimeout(long checkpointTimeout) {
    if (checkpointTimeout <= 0) {
      throw new IllegalArgumentException("Checkpoint timeout must be larger than zero");
    }
    this.checkpointTimeout = checkpointTimeout;
  }

  public long getMinPauseBetweenCheckpoints() {
    return minPauseBetweenCheckpoints;
  }

  public void setMinPauseBetweenCheckpoints(long minPauseBetweenCheckpoints) {
    if (minPauseBetweenCheckpoints < 0) {
      throw new IllegalArgumentException("Pause value must be zero or positive");
    }
    this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
  }

  public int getMaxConcurrentCheckpoints() {
    return maxConcurrentCheckpoints;
  }

  public void setMaxConcurrentCheckpoints(int maxConcurrentCheckpoints) {
    if (maxConcurrentCheckpoints < 1) {
      throw new IllegalArgumentException("The maximum number of concurrent attempts "
          + "must be at least one.");
    }
    this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
  }

}

