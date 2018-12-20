package edu.iu.dsc.tws.dashboard.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import edu.iu.dsc.tws.dashboard.repositories.ClusterRepository;
import edu.iu.dsc.tws.dashboard.repositories.JobRepository;
import edu.iu.dsc.tws.dashboard.repositories.NodeRepository;
import edu.iu.dsc.tws.dashboard.repositories.WorkerRepository;
import edu.iu.dsc.tws.dashboard.rest_models.ElementStatsResponse;

@Service
public class StatsService {

  @Autowired
  private JobRepository jobRepository;

  @Autowired
  private WorkerRepository workerRepository;

  @Autowired
  private NodeRepository nodeRepository;

  @Autowired
  private ClusterRepository clusterRepository;

  public ElementStatsResponse getElementStats() {
    ElementStatsResponse statsResponse = new ElementStatsResponse();
    statsResponse.setClusters(clusterRepository.count());
    statsResponse.setJobs(jobRepository.count());
    statsResponse.setNodes(nodeRepository.count());
    statsResponse.setWorkers(workerRepository.count());
    return statsResponse;
  }
}
