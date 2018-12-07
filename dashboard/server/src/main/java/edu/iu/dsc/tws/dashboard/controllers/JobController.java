package edu.iu.dsc.tws.dashboard.controllers;


import edu.iu.dsc.tws.dashboard.data_models.ComputeResource;
import edu.iu.dsc.tws.dashboard.data_models.JobState;
import edu.iu.dsc.tws.dashboard.rest_models.ComputeResourceScaleRequest;
import edu.iu.dsc.tws.dashboard.rest_models.StateChangeRequest;
import edu.iu.dsc.tws.dashboard.services.ComputeResourceService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import edu.iu.dsc.tws.dashboard.data_models.Job;
import edu.iu.dsc.tws.dashboard.data_models.Worker;
import edu.iu.dsc.tws.dashboard.services.JobService;
import edu.iu.dsc.tws.dashboard.services.WorkerService;

@RestController
@RequestMapping("jobs")
public class JobController {

  private static final Logger LOG = LogManager.getLogger(JobController.class);

  private final JobService jobService;

  private final WorkerService workerService;

  private final ComputeResourceService computeResourceService;

  @Autowired
  public JobController(JobService jobService, WorkerService workerService, ComputeResourceService computeResourceService) {
    this.jobService = jobService;
    this.workerService = workerService;
    this.computeResourceService = computeResourceService;
  }

  @RequestMapping(value = "/", method = RequestMethod.GET)
  public Iterable<Job> all() {
    LOG.debug("Request received to list all jobs");
    return this.jobService.getAllJobs();
  }

  @RequestMapping(value = "/", method = RequestMethod.POST,
          consumes = MediaType.APPLICATION_JSON_VALUE)
  public Job createJob(@RequestBody Job jobCreateRequest) {
    LOG.debug("Job persistent request received. {}", jobCreateRequest);
    return this.jobService.createJob(jobCreateRequest);
  }

  @RequestMapping(value = "/{jobId}/", method = RequestMethod.GET)
  public Job getJob(@PathVariable("jobId") String jobId) {
    LOG.debug("Querying single job {}", jobId);
    return this.jobService.getJobById(jobId);
  }

  @RequestMapping(value = "/{jobId}/workers/", method = RequestMethod.GET)
  public Iterable<Worker> getWorkers(@PathVariable("jobId") String jobId) {
    LOG.debug("Querying workers of job {}", jobId);
    return workerService.getAllForJob(jobId);
  }

//  @RequestMapping(value = "/{jobId}/workers/", method = RequestMethod.POST,
//          consumes = MediaType.APPLICATION_JSON_VALUE)
//  public Worker getWorkers(@PathVariable("jobId") String jobId, @RequestBody Worker worker) {
//    return workerService.createWorker(jobId, worker);
//  }

  @RequestMapping(value = "/{jobId}/state/", method = RequestMethod.POST,
          consumes = MediaType.APPLICATION_JSON_VALUE)
  public void changeState(@PathVariable String jobId,
                          @RequestBody StateChangeRequest<JobState> stateChangeRequest) {
    LOG.debug("Changing state of the job {} to {}", jobId, stateChangeRequest.getState());
    this.jobService.changeState(jobId, stateChangeRequest);
  }

  @RequestMapping(value = "/{jobId}/computeResources/", method = RequestMethod.POST,
          consumes = MediaType.APPLICATION_JSON_VALUE)
  public ComputeResource createComputeResource(@PathVariable String jobId,
                                               @RequestBody ComputeResource computeResource) {
    return computeResourceService.save(jobId, computeResource);
  }

  @RequestMapping(value = "/{jobId}/computeResources/{index}/scale/", method = RequestMethod.POST,
          consumes = MediaType.APPLICATION_JSON_VALUE)
  public void createComputeResource(@PathVariable String jobId,
                                               @PathVariable Integer index,
                                               @RequestBody ComputeResourceScaleRequest computeResourceScaleRequest) {
    computeResourceService.scale(jobId, index, computeResourceScaleRequest);
  }

  @RequestMapping(value = "/{jobId}/computeResources/{index}", method = RequestMethod.DELETE)
  public void deleteComputeResource(@PathVariable String jobId,
                                    @PathVariable Integer index) {
    computeResourceService.delete(jobId, index);
  }


//  @RequestMapping(value = "/{jobId}/beat/", method = RequestMethod.POST)
//  public void heartbeat(@PathVariable String jobId) {
//    LOG.debug("heartbeat signal received for job {}", jobId);
//    this.jobService.heartbeat(jobId);
//  }

}
