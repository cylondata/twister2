package edu.iu.dsc.tws.dashboard.services;

import edu.iu.dsc.tws.dashboard.data_models.ComputeResource;
import edu.iu.dsc.tws.dashboard.data_models.Job;
import edu.iu.dsc.tws.dashboard.data_models.composite_ids.ComputeResourceId;
import edu.iu.dsc.tws.dashboard.repositories.ComputeResourceRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
