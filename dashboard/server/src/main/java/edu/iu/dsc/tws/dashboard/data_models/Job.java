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

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.PrePersist;

import io.swagger.annotations.ApiModelProperty;

@Entity
public class Job {

  @Id
  private String jobID = UUID.randomUUID().toString();

  @Column(nullable = false)
  private String jobName;

  @ApiModelProperty(hidden = true)
  @Column
  private Date heartbeatTime; //job master heartbeat

  @ApiModelProperty(hidden = true)
  @OneToMany(fetch = FetchType.EAGER, mappedBy = "job",
          orphanRemoval = true)
  private Set<Worker> workers = new HashSet<>();

  @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER,
      mappedBy = "job", orphanRemoval = true)
  private Set<ComputeResource> computeResources = new HashSet<>();

  @ManyToOne(optional = false)
  private Node node;

  @Column
  @Enumerated(EnumType.STRING)
  private JobState state = JobState.STARTING;

  @Column
  private int numberOfWorkers = 0;

  @Column
  private String workerClass;

  @Column
  private Date createdTime;

  public Date getCreatedTime() {
    return createdTime;
  }

  public void setCreatedTime(Date createdTime) {
    this.createdTime = createdTime;
  }

  public String getWorkerClass() {
    return workerClass;
  }

  public void setWorkerClass(String workerClass) {
    this.workerClass = workerClass;
  }

  public int getNumberOfWorkers() {
    return numberOfWorkers;
  }

  public void setNumberOfWorkers(int numberOfWorkers) {
    this.numberOfWorkers = numberOfWorkers;
  }

  public Node getNode() {
    return node;
  }

  public void setNode(Node node) {
    this.node = node;
  }

  public Set<ComputeResource> getComputeResources() {
    return computeResources;
  }

  public void setComputeResources(Set<ComputeResource> computeResources) {
    this.computeResources = computeResources;
  }

  public JobState getState() {
    return state;
  }

  public void setState(JobState state) {
    this.state = state;
  }

  public String getJobID() {
    return jobID;
  }

  public void setJobID(String jobID) {
    this.jobID = jobID;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public Date getHeartbeatTime() {
    return heartbeatTime;
  }

  public void setHeartbeatTime(Date heartbeatTime) {
    this.heartbeatTime = heartbeatTime;
  }

  public Set<Worker> getWorkers() {
    return workers;
  }

  public void setWorkers(Set<Worker> workers) {
    this.workers = workers;
  }

  @PrePersist
  public void prePersist() {
    this.createdTime = new Date();
  }

  @Override
  public String toString() {
    return "Job{"
            + "jobID='" + jobID + '\''
            + ", jobName='" + jobName + '\''
            + ", heartbeatTime=" + heartbeatTime
            + ", workers=" + workers
            + '}';
  }
}
