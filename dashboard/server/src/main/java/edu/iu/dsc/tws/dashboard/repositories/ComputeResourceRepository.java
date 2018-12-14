package edu.iu.dsc.tws.dashboard.repositories;

import edu.iu.dsc.tws.dashboard.data_models.ComputeResource;
import edu.iu.dsc.tws.dashboard.data_models.composite_ids.ComputeResourceId;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

public interface ComputeResourceRepository extends CrudRepository<ComputeResource, ComputeResourceId> {

  ComputeResource findDistinctByJob_JobIDAndIndex(String jobId, Integer index);

  void deleteByJob_JobIDAndIndex(String jobId, Integer index);

  @Modifying
  @Query("update ComputeResource cr set cr.instances=?3 where cr.job.jobID=?1 and cr.index=?2")
  int scale(String jobId, Integer index, Integer instances);
}
