package edu.iu.dsc.tws.dashboard.controllers;

import edu.iu.dsc.tws.dashboard.data_models.Node;
import edu.iu.dsc.tws.dashboard.rest_models.StateChangeRequest;
import edu.iu.dsc.tws.dashboard.services.NodeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("nodes")
public class NodeController {

    @Autowired
    private NodeService nodeService;

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public Iterable<Node> getAll() {
        return nodeService.getAll();
    }

    @RequestMapping(value = "/", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public Node createNode(@RequestBody Node node) {
        return nodeService.createNode(node);
    }

    @RequestMapping(value = "/{nodeId}/state/", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public void changeState(@PathVariable Long nodeId, @RequestBody StateChangeRequest stateChangeRequest) {
        this.nodeService.changeState(nodeId, stateChangeRequest);
    }
}
