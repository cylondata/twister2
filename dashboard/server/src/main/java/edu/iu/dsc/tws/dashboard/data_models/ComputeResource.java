package edu.iu.dsc.tws.dashboard.data_models;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import edu.iu.dsc.tws.dashboard.data_models.composite_ids.ComputeResourceId;

import io.swagger.annotations.ApiModelProperty;

@Entity
@IdClass(ComputeResourceId.class)
public class ComputeResource {

  @Column
  private Double cpu = 0d;

  @Column
  private Integer ram = 0;

  @Column
  private Double disk = 0d;

  @Column
  private Integer instances = 0;

  @Column
  private Boolean scalable = Boolean.FALSE;

  @Id
  @Column
  private Integer index = 0;

  @ApiModelProperty(hidden = true)
  @Id
  @ManyToOne(optional = false)
  @JoinColumn
  @JsonIgnoreProperties({"workers", "description", "heartbeatTime", "state", "computeResources",
      "node", "numberOfWorkers", "workerClass"})
  private Job job;

  public Boolean isScalable() {
    return scalable;
  }

  public void setScalable(Boolean scalable) {
    this.scalable = scalable;
  }

  public Job getJob() {
    return job;
  }

  public void setJob(Job job) {
    this.job = job;
  }

  public Double getCpu() {
    return cpu;
  }

  public void setCpu(Double cpu) {
    this.cpu = cpu;
  }

  public Integer getRam() {
    return ram;
  }

  public void setRam(Integer ram) {
    this.ram = ram;
  }

  public Double getDisk() {
    return disk;
  }

  public void setDisk(Double disk) {
    this.disk = disk;
  }

  public Integer getInstances() {
    return instances;
  }

  public void setInstances(Integer instances) {
    this.instances = instances;
  }

  public Integer getIndex() {
    return index;
  }

  public void setIndex(Integer index) {
    this.index = index;
  }
}
