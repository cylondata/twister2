package edu.iu.dsc.tws.dashboard.services;

import java.util.Optional;

import javax.persistence.EntityNotFoundException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import edu.iu.dsc.tws.dashboard.data_models.ComputeResource;
import edu.iu.dsc.tws.dashboard.data_models.Job;
import edu.iu.dsc.tws.dashboard.data_models.composite_ids.ComputeResourceId;
import edu.iu.dsc.tws.dashboard.repositories.ComputeResourceRepository;
import edu.iu.dsc.tws.dashboard.rest_models.ComputeResourceScaleRequest;

@Service
public class ComputeResourceService {

  private final ComputeResourceRepository computeResourceRepository;

  private final JobService jobService;

  @Autowired
  public ComputeResourceService(
          ComputeResourceRepository computeResourceRepository, JobService jobService) {
    this.computeResourceRepository = computeResourceRepository;
    this.jobService = jobService;
  }

  public ComputeResource save(String jobId, ComputeResource computeResource) {
    Job jobById = jobService.getJobById(jobId);
    computeResource.setJob(jobById);
    return computeResourceRepository.save(computeResource);
  }

  public ComputeResource findById(ComputeResourceId computeResourceId) {
    Optional<ComputeResource> byId = computeResourceRepository.findById(
            computeResourceId
    );
    if (byId.isPresent()) {
      return byId.get();
    }
    this.throwNoSuchComputeResourceException(
            computeResourceId.getJob(),
            computeResourceId.getIndex()
    );
    return null;
  }

  public ComputeResource findById(String jobId, Integer index) {
    return this.findById(
            this.createComputerResourceId(jobId, index)
    );
  }

  @Transactional
  public void delete(String jobId, Integer index) {
    computeResourceRepository.deleteById(
            this.createComputerResourceId(jobId, index)
    );
  }

  public ComputeResourceId createComputerResourceId(String jobId, Integer index) {
    ComputeResourceId computeResourceId = new ComputeResourceId();
    computeResourceId.setIndex(index);
    computeResourceId.setJob(jobId);
    return computeResourceId;
  }

  @Transactional
  public void scale(String jobId, Integer index, ComputeResourceScaleRequest computeResourceScaleRequest) {
    int scaledAmount = this.computeResourceRepository.scale(jobId, index, computeResourceScaleRequest.getInstances());
    if (scaledAmount == 0) {
      this.throwNoSuchComputeResourceException(jobId, index);
    }
  }

  private void throwNoSuchComputeResourceException(String jobId, Integer index) {
    throw new EntityNotFoundException("No such compute resource defined with id"
            + index + " for job " + jobId);
  }
}
