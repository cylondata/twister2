package edu.iu.dsc.tws.dashboard.repositories;

import edu.iu.dsc.tws.dashboard.data_models.ComputeResource;
import edu.iu.dsc.tws.dashboard.data_models.composite_ids.ComputeResourceId;
import org.springframework.data.repository.CrudRepository;

public interface ComputeResourceRepository extends CrudRepository<ComputeResource, ComputeResourceId> {
  ComputeResource findDistinctByJob_JobIdAndIndex(String jobId, Integer index);

  void deleteByJob_JobIdAndIndex(String jobId, Integer index);

}
