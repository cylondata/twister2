package edu.iu.dsc.tws.dashboard.repositories;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import edu.iu.dsc.tws.dashboard.data_models.EntityState;
import edu.iu.dsc.tws.dashboard.data_models.Node;

public interface NodeRepository extends CrudRepository<Node, Long> {
  @Modifying
  @Query("update Node node set node.state=?2 where node.id=?1")
  int changeNodeState(Long nodeId, EntityState entityState);

  Iterable<Node> findAllByCluster_Id(Long clusterId);
}
