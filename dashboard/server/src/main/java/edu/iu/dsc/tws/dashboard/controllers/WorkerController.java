package edu.iu.dsc.tws.dashboard.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import edu.iu.dsc.tws.dashboard.data_models.Worker;
import edu.iu.dsc.tws.dashboard.data_models.WorkerState;
import edu.iu.dsc.tws.dashboard.rest_models.StateChangeRequest;
import edu.iu.dsc.tws.dashboard.rest_models.WorkerCreateRequest;
import edu.iu.dsc.tws.dashboard.services.WorkerService;

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

  @RequestMapping(value = "/{jobId}/{workerId}/", method = RequestMethod.GET)
  public Worker getAllWorkers(@PathVariable("jobId") String jobId,
                              @PathVariable("workerId") Long workerId) {
    return workerService.getWorkerById(jobId, workerId);
  }

  @RequestMapping(value = "/{jobId}/{workerId}/state/", method = RequestMethod.POST,
          consumes = MediaType.APPLICATION_JSON_VALUE)
  public void changeState(@PathVariable("jobId") String jobId,
                          @PathVariable("workerId") Long workerId,
                          @RequestBody StateChangeRequest<WorkerState> stateChangeRequest) {
    this.workerService.changeState(jobId, workerId, stateChangeRequest);
  }

  @RequestMapping(value = "/{jobId}/{workerId}/beat/", method = RequestMethod.POST)
  public void heartbeat(@PathVariable("jobId") String jobId,
                        @PathVariable("workerId") Long workerId) {
    this.workerService.heartbeat(jobId, workerId);
  }

  @RequestMapping(value = "/stats/", method = RequestMethod.GET)
  public Object getStateStats() {
    return this.workerService.getStateStats();
  }
}
