package edu.iu.dsc.tws.dashboard.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import edu.iu.dsc.tws.dashboard.data_models.Node;
import edu.iu.dsc.tws.dashboard.repositories.NodeRepository;
import edu.iu.dsc.tws.dashboard.rest_models.StateChangeRequest;

@Service
public class NodeService {

  @Autowired
  private NodeRepository nodeRepository;

  public Iterable<Node> getAll() {
    return nodeRepository.findAll();
  }

  public Node createNode(Node node) {
    return this.nodeRepository.save(node);
  }

  public void changeState(Long nodeId, StateChangeRequest stateChangeRequest) {
    this.nodeRepository.changeNodeState(nodeId, stateChangeRequest.getEntityState());
  }

  public Iterable<Node> getNodesOfCluster(Long clusterId) {
    return this.nodeRepository.findAllByCluster_Id(clusterId);
  }
}
