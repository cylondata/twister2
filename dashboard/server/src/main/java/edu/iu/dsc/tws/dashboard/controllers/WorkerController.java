package edu.iu.dsc.tws.dashboard.controllers;

import edu.iu.dsc.tws.dashboard.data_models.Worker;
import edu.iu.dsc.tws.dashboard.data_models.WorkerState;
import edu.iu.dsc.tws.dashboard.rest_models.StateChangeRequest;
import edu.iu.dsc.tws.dashboard.rest_models.WorkerCreateRequest;
import edu.iu.dsc.tws.dashboard.services.WorkerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("workers")
public class WorkerController {

  private final WorkerService workerService;

  @Autowired
  public WorkerController(WorkerService workerService) {
    this.workerService = workerService;
  }

  @RequestMapping(value = "/", method = RequestMethod.GET)
  public Iterable<Worker> getAllWorkers() {
    return workerService.getAllWorkers();
  }

  @RequestMapping(value = "/", method = RequestMethod.POST,
          consumes = MediaType.APPLICATION_JSON_VALUE)
  public Worker createWorker(@RequestBody WorkerCreateRequest workerCreateRequest) {
    return this.workerService.createWorker(workerCreateRequest);
  }

  @RequestMapping(value = "/{jobId}/{workerId}/state/", method = RequestMethod.POST,
          consumes = MediaType.APPLICATION_JSON_VALUE)
  public void changeState(@PathVariable String jobId,
                          @PathVariable Long workerId,
                          @RequestBody StateChangeRequest<WorkerState> stateChangeRequest) {
    this.workerService.changeState(jobId, workerId, stateChangeRequest);
  }
}
