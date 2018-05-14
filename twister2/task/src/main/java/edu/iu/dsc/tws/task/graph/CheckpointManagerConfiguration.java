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
package edu.iu.dsc.tws.task.graph;

import java.io.Serializable;
import java.util.Objects;

public class CheckpointManagerConfiguration implements Serializable {
  private static final long serialVersionUID = 2L;

  private final long checkpointInterval;

  private final long checkpointTimeout;

  private final long minPauseBetweenCheckpoints;

  private final int maxConcurrentCheckpoints;

  //private final CheckpointRetentionPolicy checkpointRetentionPolicy;

  private final boolean isExactlyOnce;

  public CheckpointManagerConfiguration(
      long checkpointInterval,
      long checkpointTimeout,
      long minPauseBetweenCheckpoints,
      int maxConcurrentCheckpoints,
      boolean isExactlyOnce) {

    // sanity checks
    if (checkpointInterval < 1 || checkpointTimeout < 1
        || minPauseBetweenCheckpoints < 0 || maxConcurrentCheckpoints < 1) {
      throw new IllegalArgumentException();
    }

    this.checkpointInterval = checkpointInterval;
    this.checkpointTimeout = checkpointTimeout;
    this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
    this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
    this.isExactlyOnce = isExactlyOnce;
  }

  public long getCheckpointInterval() {
    return checkpointInterval;
  }

  public long getCheckpointTimeout() {
    return checkpointTimeout;
  }

  public long getMinPauseBetweenCheckpoints() {
    return minPauseBetweenCheckpoints;
  }

  public int getMaxConcurrentCheckpoints() {
    return maxConcurrentCheckpoints;
  }

  public boolean isExactlyOnce() {
    return isExactlyOnce;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CheckpointManagerConfiguration that = (CheckpointManagerConfiguration) o;
    return checkpointInterval == that.checkpointInterval
        && checkpointTimeout == that.checkpointTimeout
        && minPauseBetweenCheckpoints == that.minPauseBetweenCheckpoints
        && maxConcurrentCheckpoints == that.maxConcurrentCheckpoints
        && isExactlyOnce == that.isExactlyOnce;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        checkpointInterval,
        checkpointTimeout,
        minPauseBetweenCheckpoints,
        maxConcurrentCheckpoints,
        isExactlyOnce);
  }

  @Override
  public String toString() {
    return "JobCheckpointingConfiguration{"
        + "checkpointInterval=" + checkpointInterval
        + ", checkpointTimeout=" + checkpointTimeout
        + ", minPauseBetweenCheckpoints=" + minPauseBetweenCheckpoints
        + ", maxConcurrentCheckpoints=" + maxConcurrentCheckpoints
        + '}';
  }
}
