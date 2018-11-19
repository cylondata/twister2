package edu.iu.dsc.tws.dashboard.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import edu.iu.dsc.tws.dashboard.data_models.Cluster;
import edu.iu.dsc.tws.dashboard.data_models.Node;
import edu.iu.dsc.tws.dashboard.services.ClusterService;
import edu.iu.dsc.tws.dashboard.services.NodeService;

@RestController
@RequestMapping("clusters")
public class ClusterController {

  @Autowired
  private ClusterService clusterService;

  @Autowired
  private NodeService nodeService;

  @RequestMapping(value = "/", method = RequestMethod.GET)
  public Iterable<Cluster> getAll() {
    return clusterService.getAllClusters();
  }

  @RequestMapping(value = "/", method = RequestMethod.POST,
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public Cluster createCluster(@RequestBody Cluster cluster) {
    return clusterService.createCluster(cluster);
  }

  @RequestMapping(value = "/{id}/", method = RequestMethod.GET)
  public Cluster getCluster(@PathVariable("id") Long id) {
    return clusterService.getClusterById(id);
  }

  @RequestMapping(value = "/{id}/nodes/", method = RequestMethod.GET)
  public Iterable<Node> getNodes(@PathVariable Long id) {
    return this.nodeService.getNodesOfCluster(id);
  }
}
