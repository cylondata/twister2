package edu.iu.dsc.tws.dashboard.repositories;

import org.springframework.data.repository.CrudRepository;

import edu.iu.dsc.tws.dashboard.data_models.Cluster;

public interface ClusterRepository extends CrudRepository<Cluster, String> {
}
