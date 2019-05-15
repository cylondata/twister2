package edu.iu.dsc.tws.dashboard.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import edu.iu.dsc.tws.dashboard.data_models.Node;
import edu.iu.dsc.tws.dashboard.services.NodeService;

@RestController
@RequestMapping("nodes")
public class NodeController {

  private final NodeService nodeService;

  @Autowired
  public NodeController(NodeService nodeService) {
    this.nodeService = nodeService;
  }

  @RequestMapping(value = "/", method = RequestMethod.GET)
  public Iterable<Node> getAll() {
    return nodeService.getAll();
  }

  @RequestMapping(value = "/", method = RequestMethod.POST,
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public Node createNode(@RequestBody Node node) {
    return nodeService.createNode(node);
  }

//  @RequestMapping(value = "/{nodeId}/state/", method = RequestMethod.POST,
//      consumes = MediaType.APPLICATION_JSON_VALUE)
//  public void changeState(@PathVariable Long nodeId,
//                          @RequestBody StateChangeRequest stateChangeRequest) {
//    this.nodeService.changeState(nodeId, stateChangeRequest);
//  }
}
