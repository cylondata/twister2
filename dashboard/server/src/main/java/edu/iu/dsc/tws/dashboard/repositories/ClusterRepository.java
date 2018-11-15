package edu.iu.dsc.tws.dashboard.repositories;

import edu.iu.dsc.tws.dashboard.data_models.Cluster;
import org.springframework.data.repository.CrudRepository;

public interface ClusterRepository extends CrudRepository<Cluster, Long> {
}
