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

import java.util.Date;
import java.util.Optional;

import javax.persistence.EntityNotFoundException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import edu.iu.dsc.tws.dashboard.data_models.Job;
import edu.iu.dsc.tws.dashboard.repositories.JobRepository;
import edu.iu.dsc.tws.dashboard.rest_models.StateChangeRequest;

@Service
public class JobService {

  @Autowired
  private JobRepository jobRepository;

  public Job createJob(Job job) {
    job.getWorkers().forEach(worker -> worker.setJob(job));
    return jobRepository.save(job);
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
  public void changeState(String jobId, StateChangeRequest stateChangeRequest) {
    this.jobRepository.changeJobState(jobId, stateChangeRequest.getEntityState());
  }

  @Transactional
  public void heartbeat(String jobId) {
    this.jobRepository.heartbeat(jobId, new Date());
  }
}
