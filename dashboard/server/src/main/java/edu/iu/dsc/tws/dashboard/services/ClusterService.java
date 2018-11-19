package edu.iu.dsc.tws.dashboard.services;

import java.util.Optional;

import javax.persistence.EntityNotFoundException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import edu.iu.dsc.tws.dashboard.data_models.Cluster;
import edu.iu.dsc.tws.dashboard.repositories.ClusterRepository;

@Service
public class ClusterService {

  @Autowired
  private ClusterRepository clusterRepository;

  public Iterable<Cluster> getAllClusters() {
    return clusterRepository.findAll();
  }

  public Cluster getClusterById(Long id) {
    Optional<Cluster> byId = clusterRepository.findById(id);
    if (byId.isPresent()) {
      return byId.get();
    }
    throw new EntityNotFoundException("Cluster not found with ID " + id);
  }

  public Cluster createCluster(Cluster cluster) {
    cluster.getNodes().forEach(node -> node.setCluster(cluster));
    return this.clusterRepository.save(cluster);
  }
}
