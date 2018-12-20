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
package edu.iu.dsc.tws.dashboard.services;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import javax.persistence.EntityNotFoundException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import edu.iu.dsc.tws.dashboard.data_models.ComputeResource;
import edu.iu.dsc.tws.dashboard.data_models.Job;
import edu.iu.dsc.tws.dashboard.data_models.JobState;
import edu.iu.dsc.tws.dashboard.data_models.Node;
import edu.iu.dsc.tws.dashboard.repositories.JobRepository;
import edu.iu.dsc.tws.dashboard.rest_models.ScaleWorkersRequest;
import edu.iu.dsc.tws.dashboard.rest_models.StateChangeRequest;

@Service
public class JobService {

  private final JobRepository jobRepository;

  private final NodeService nodeService;

  @Autowired
  private ComputeResourceService computeResourceService;

  @Autowired
  public JobService(JobRepository jobRepository, NodeService nodeService) {
    this.jobRepository = jobRepository;
    this.nodeService = nodeService;
  }

  public Job createJob(Job job) {
    job.getWorkers().forEach(worker -> worker.setJob(job));
    job.getComputeResources().forEach(computeResource -> computeResource.setJob(job));

    //create non existing nodes : todo not appropriate, resolve once twister2 support nodes
    //if node is defined without rack and data center, replace them with prefix+jobId

    if (StringUtils.isEmpty(job.getNode().getDataCenter())) {
      job.getNode().setDataCenter("dc-" + job.getJobID());
    }
    if (StringUtils.isEmpty(job.getNode().getRack())) {
      job.getNode().setRack("rk-" + job.getJobID());
    }
    Node node = nodeService.createNode(job.getNode());
    job.setNode(node);

    job.setHeartbeatTime(Calendar.getInstance().getTime());

    return jobRepository.save(job);
  }

  public Page<Job> searchJobs(List<JobState> states, String keyword, int page) {
    PageRequest pageRequest = PageRequest.of(page, 25);
    return this.jobRepository.findAllByStateInAndJobNameContainingOrderByCreatedTimeDesc(
        states,
        keyword,
        pageRequest
    );
  }

  public Job getJobById(String jobId) {
    Optional<Job> byId = jobRepository.findById(jobId);
    if (byId.isPresent()) {
      return byId.get();
    }
    throw new EntityNotFoundException("No Job found with ID " + jobId);
  }

  public Iterable<Job> getAllJobs() {
    return this.jobRepository.findAll();
  }

  @Transactional
  public void changeState(String jobId, StateChangeRequest<JobState> stateChangeRequest) {
    int changeJobState = this.jobRepository.changeJobState(jobId, stateChangeRequest.getState());
    if (changeJobState == 0) {
      throw new EntityNotFoundException("No Job found with ID " + jobId);
    }
  }

  @Transactional
  public void scale(String jobId, ScaleWorkersRequest scaleWorkersRequest) {
    ComputeResource cr = this.computeResourceService
        .getScalableComputeResourceForJob(jobId);

    cr.setInstances(cr.getInstances() + scaleWorkersRequest.getChange());
    this.computeResourceService.save(cr);

    this.jobRepository.changeNumberOfWorkers(jobId, scaleWorkersRequest.getNumberOfWorkers());
  }

  @Transactional
  public void heartbeat(String jobId) {
    this.jobRepository.heartbeat(jobId, new Date());
  }

  public Object getStateStats() {
    return this.jobRepository.getStateStats();
  }
}
