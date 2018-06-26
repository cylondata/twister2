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

import java.io.Serializable;

public class CheckpointMetrics implements Serializable {

  private static final long serialVersionUID = 1L;

  private long bytesBufferedInAlignment;

  private long alignmentDurationNanos;

  private long syncDurationMillis;

  private long asyncDurationMillis;

  public CheckpointMetrics() {
    this(-1L, -1L, -1L, -1L);
  }

  public CheckpointMetrics(
      long bytesBufferedInAlignment,
      long alignmentDurationNanos,
      long syncDurationMillis,
      long asyncDurationMillis) {

    this.bytesBufferedInAlignment = bytesBufferedInAlignment;
    this.alignmentDurationNanos = alignmentDurationNanos;
    this.syncDurationMillis = syncDurationMillis;
    this.asyncDurationMillis = asyncDurationMillis;
  }

  public long getBytesBufferedInAlignment() {
    return bytesBufferedInAlignment;
  }

  public CheckpointMetrics setBytesBufferedInAlignment(long newBytesBufferedInAlignment) {
    this.bytesBufferedInAlignment = newBytesBufferedInAlignment;
    return this;
  }

  public long getAlignmentDurationNanos() {
    return alignmentDurationNanos;
  }

  public CheckpointMetrics setAlignmentDurationNanos(long newAlignmentDurationNanos) {
    this.alignmentDurationNanos = newAlignmentDurationNanos;
    return this;
  }

  public long getSyncDurationMillis() {
    return syncDurationMillis;
  }

  public CheckpointMetrics setSyncDurationMillis(long newSyncDurationMillis) {
    this.syncDurationMillis = newSyncDurationMillis;
    return this;
  }

  public long getAsyncDurationMillis() {
    return asyncDurationMillis;
  }

  public CheckpointMetrics setAsyncDurationMillis(long newAsyncDurationMillis) {
    this.asyncDurationMillis = newAsyncDurationMillis;
    return this;
  }
}
