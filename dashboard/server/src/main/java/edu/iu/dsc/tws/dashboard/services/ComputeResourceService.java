package edu.iu.dsc.tws.dashboard.services;

import edu.iu.dsc.tws.dashboard.data_models.ComputeResource;
import edu.iu.dsc.tws.dashboard.data_models.Job;
import edu.iu.dsc.tws.dashboard.data_models.composite_ids.ComputeResourceId;
import edu.iu.dsc.tws.dashboard.repositories.ComputeResourceRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityNotFoundException;
import java.util.Optional;

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
    throw new EntityNotFoundException("No such compute resource defined with id"
            + computeResourceId.getIndex() + " for job " + computeResourceId.getJob());
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
}
