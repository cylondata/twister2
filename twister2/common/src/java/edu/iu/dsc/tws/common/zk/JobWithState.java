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
package edu.iu.dsc.tws.common.zk;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.iu.dsc.tws.proto.system.job.JobAPI;

public class JobWithState {
  public static final Logger LOG = Logger.getLogger(JobWithState.class.getName());

  private JobAPI.Job job;
  private JobAPI.JobState state;

  public JobWithState(JobAPI.Job job, JobAPI.JobState state) {
    this.job = job;
    this.state = state;
  }

  public JobAPI.Job getJob() {
    return job;
  }

  public JobAPI.JobState getState() {
    return state;
  }

  public void setState(JobAPI.JobState state) {
    this.state = state;
  }

  /**
   * first 4 bytes state length
   * next comes state string
   * remaining is job object
   * @return
   */
  public byte[] toByteArray() {
    byte[] stateNameBytes = state.name().getBytes();
    byte[] stateLengthBytes = Ints.toByteArray(stateNameBytes.length);

    byte[] jobBytes = job.toByteArray();

    return Bytes.concat(stateLengthBytes, stateNameBytes, jobBytes);
  }

  public static JobWithState decode(byte[] encodedBytes) {
    if (encodedBytes == null) {
      return null;
    }

    // first 4 bytes is the length of job state
    int stateLength = Ints.fromByteArray(encodedBytes);
    String stateName = new String(encodedBytes, 4, stateLength);
    JobAPI.JobState state = JobAPI.JobState.valueOf(stateName);

    try {
      JobAPI.Job job = JobAPI.Job.newBuilder()
          .mergeFrom(encodedBytes, 4 + stateLength, encodedBytes.length - 4 - stateLength)
          .build();
      return new JobWithState(job, state);
    } catch (InvalidProtocolBufferException e) {
      LOG.log(Level.SEVERE, "Could not decode received byte array as a JobAPI.Job object", e);
      return null;
    }
  }

}
