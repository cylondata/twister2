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

  public Cluster getClusterById(String id) {
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

  /**
   * This util method creates a cluster for a given data center name.
   * Assumes data center names are unique. Will be removed when twister2 support clusters
   */
  public Cluster createOrGetCluster(String dataCenter) {
    Optional<Cluster> byId = clusterRepository.findById(dataCenter);
    if (byId.isPresent()) {
      return byId.get();
    } else {
      Cluster cluster = new Cluster();
      cluster.setId(dataCenter);
      cluster.setName(dataCenter);
      cluster.setDescription("This cluster was created automatically "
          + "based on the data center name");
      return this.createCluster(cluster);
    }
  }
}
