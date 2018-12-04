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
package edu.iu.dsc.tws.dashboard.data_models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import edu.iu.dsc.tws.dashboard.data_models.composite_ids.WorkerId;
import io.swagger.annotations.ApiModelProperty;

import javax.persistence.*;

@Entity
@IdClass(WorkerId.class)
public class Worker {

  @Id
  private Long workerId;

  @Column
  private String workerIP;

  @Column
  private Integer workerPort;

  @ApiModelProperty(hidden = true)
  @Id
  @ManyToOne(optional = false)
  @JoinColumn(referencedColumnName = "jobId")
  @JsonIgnoreProperties({"workers", "description", "heartbeatTime", "state", "computeResources", "node"})
  private Job job;

  @Column
  @Enumerated(EnumType.STRING)
  private WorkerState state = WorkerState.STARTING;

  @ManyToOne(optional = false, cascade = CascadeType.PERSIST)
  private ComputeResource computeResource;

  @ManyToOne(optional = false)
  private Node node;

  public Node getNode() {
    return node;
  }

  public void setNode(Node node) {
    this.node = node;
  }

  public ComputeResource getComputeResource() {
    return computeResource;
  }

  public void setComputeResource(ComputeResource computeResource) {
    this.computeResource = computeResource;
  }

  public WorkerState getState() {
    return state;
  }

  public void setState(WorkerState state) {
    this.state = state;
  }

  public Job getJob() {
    return job;
  }

  public void setJob(Job job) {
    this.job = job;
  }

  public Long getWorkerId() {
    return workerId;
  }

  public void setWorkerId(Long workerId) {
    this.workerId = workerId;
  }

  public String getWorkerIP() {
    return workerIP;
  }

  public void setWorkerIP(String workerIP) {
    this.workerIP = workerIP;
  }

  public Integer getWorkerPort() {
    return workerPort;
  }

  public void setWorkerPort(Integer workerPort) {
    this.workerPort = workerPort;
  }

  @Override
  public String toString() {
    return "Worker{"
            + "workerId=" + workerId
            + ", workerIP='" + workerIP + '\''
            + ", workerPort=" + workerPort
            + ", job=" + job
            + '}';
  }
}
