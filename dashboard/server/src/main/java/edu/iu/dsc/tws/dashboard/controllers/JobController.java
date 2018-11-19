package edu.iu.dsc.tws.dashboard.controllers;


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
import edu.iu.dsc.tws.dashboard.rest_models.StateChangeRequest;
import edu.iu.dsc.tws.dashboard.services.JobService;
import edu.iu.dsc.tws.dashboard.services.WorkerService;

@RestController
@RequestMapping("jobs")
public class JobController {

  private static final Logger LOG = LogManager.getLogger(JobController.class);

  @Autowired
  private JobService jobService;

  @Autowired
  private WorkerService workerService;

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

  @RequestMapping(value = "/{jobId}/state/", method = RequestMethod.POST,
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public void changeState(@PathVariable String jobId,
                          @RequestBody StateChangeRequest stateChangeRequest) {
    LOG.debug("Changing state of the job {} to {}", jobId, stateChangeRequest.getEntityState());
    this.jobService.changeState(jobId, stateChangeRequest);
  }


  @RequestMapping(value = "/{jobId}/beat/", method = RequestMethod.POST)
  public void heartbeat(@PathVariable String jobId) {
    LOG.debug("heartbeat signal received for job {}", jobId);
    this.jobService.heartbeat(jobId);
  }

}
