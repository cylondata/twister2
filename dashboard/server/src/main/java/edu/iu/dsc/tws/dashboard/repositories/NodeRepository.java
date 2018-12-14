package edu.iu.dsc.tws.dashboard.repositories;

import org.springframework.data.repository.CrudRepository;

import edu.iu.dsc.tws.dashboard.data_models.Node;
import edu.iu.dsc.tws.dashboard.data_models.composite_ids.NodeId;

public interface NodeRepository extends CrudRepository<Node, NodeId> {
//  @Modifying
//  @Query("update Node node set node.state=?2 where node.id=?1")
//  int changeNodeState(Long nodeId, EntityState entityState);

  Iterable<Node> findAllByCluster_Id(Long clusterId);
}
