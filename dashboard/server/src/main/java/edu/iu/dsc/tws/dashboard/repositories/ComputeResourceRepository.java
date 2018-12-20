package edu.iu.dsc.tws.dashboard.repositories;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import edu.iu.dsc.tws.dashboard.data_models.ComputeResource;
import edu.iu.dsc.tws.dashboard.data_models.composite_ids.ComputeResourceId;

public interface ComputeResourceRepository
    extends CrudRepository<ComputeResource, ComputeResourceId> {

  ComputeResource findDistinctByJob_JobIDAndIndex(String jobId, Integer index);

  ComputeResource findDistinctByJob_JobIDAndScalable(String jobId, Boolean scalable);

  void deleteByJob_JobIDAndIndex(String jobId, Integer index);

  @Modifying
  @Query("update ComputeResource cr set cr.instances=?3 where cr.job.jobID=?1 and cr.index=?2")
  int scale(String jobId, Integer index, Integer instances);
}
