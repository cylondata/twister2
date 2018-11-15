package edu.iu.dsc.tws.dashboard.controllers;

import edu.iu.dsc.tws.dashboard.data_models.Cluster;
import edu.iu.dsc.tws.dashboard.services.ClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("clusters")
public class ClusterController {

    @Autowired
    private ClusterService clusterService;

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public Iterable<Cluster> getAll() {
        return clusterService.getAllClusters();
    }

    @RequestMapping(value = "/", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public Cluster createCluster(@RequestBody Cluster cluster) {
        return clusterService.createCluster(cluster);
    }

    @RequestMapping(value = "/{id}/", method = RequestMethod.GET)
    public Cluster getCluster(@PathVariable("id") Long id) {
        return clusterService.getClusterById(id);
    }
}
