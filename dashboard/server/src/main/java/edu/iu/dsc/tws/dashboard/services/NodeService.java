package edu.iu.dsc.tws.dashboard.services;

import edu.iu.dsc.tws.dashboard.data_models.Cluster;
import edu.iu.dsc.tws.dashboard.data_models.composite_ids.NodeId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import edu.iu.dsc.tws.dashboard.data_models.Node;
import edu.iu.dsc.tws.dashboard.repositories.NodeRepository;

import java.util.Optional;

@Service
public class NodeService {

  @Autowired
  private NodeRepository nodeRepository;

  @Autowired
  private ClusterService clusterService;

  public Iterable<Node> getAll() {
    return nodeRepository.findAll();
  }

  public Node createNode(Node node) {
    Optional<Node> byId = nodeRepository.findById(this.createNodeId(node));
    if (byId.isPresent()) {
      return byId.get();
    }

    //creating a virtual cluster till clusters are fully supported in twister2
    if (node.getCluster() == null) {
      Cluster cluster = clusterService.createOrGetCluster(node.getDataCenter());
      node.setCluster(cluster);
    }

    return this.nodeRepository.save(node);
  }

  public NodeId createNodeId(Node node) {
    NodeId nodeId = new NodeId();
    nodeId.setDataCenter(node.getDataCenter());
    nodeId.setIp(node.getIp());
    nodeId.setRack(node.getRack());
    return nodeId;
  }

//  public void changeState(Long nodeId, JobStateChangeRequest stateChangeRequest) {
//    this.nodeRepository.changeNodeState(nodeId, stateChangeRequest.getJobState());
//  }

  public Iterable<Node> getNodesOfCluster(Long clusterId) {
    return this.nodeRepository.findAllByCluster_Id(clusterId);
  }
}
