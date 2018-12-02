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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Entity
public class Worker {

  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  private Long id;

  @Column
  private String host;

  @Column
  private Integer port;

  @Column
  private Double cpuAllocation;

  @Column
  private Double memoryAllocation;

  @Column
  @Enumerated(EnumType.STRING)
  private EntityState state;

  public EntityState getState() {
    return state;
  }

  public void setState(EntityState state) {
    this.state = state;
  }

  @ManyToOne(optional = false)
  @JoinColumn(referencedColumnName = "jobId")
  @JsonIgnoreProperties({"workers", "description", "heartbeatTime", "state"})
  private Job job;

  public Job getJob() {
    return job;
  }

  public void setJob(Job job) {
    this.job = job;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    this.port = port;
  }

  public Double getCpuAllocation() {
    return cpuAllocation;
  }

  public void setCpuAllocation(Double cpuAllocation) {
    this.cpuAllocation = cpuAllocation;
  }

  public Double getMemoryAllocation() {
    return memoryAllocation;
  }

  public void setMemoryAllocation(Double memoryAllocation) {
    this.memoryAllocation = memoryAllocation;
  }

  @Override
  public String toString() {
    return "Worker{"
            + "id=" + id
            + ", host='" + host + '\''
            + ", port=" + port
            + ", cpuAllocation=" + cpuAllocation
            + ", memoryAllocation=" + memoryAllocation
            + ", job=" + job
            + '}';
  }
}
