package edu.iu.dsc.tws.dashboard.controllers;

import edu.iu.dsc.tws.dashboard.data_models.Job;
import edu.iu.dsc.tws.dashboard.data_models.Worker;
import edu.iu.dsc.tws.dashboard.services.JobService;
import edu.iu.dsc.tws.dashboard.services.WorkerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("jobs")
public class JobController {

    @Autowired
    private JobService jobService;

    @Autowired
    private WorkerService workerService;

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public Iterable<Job> all() {
        return this.jobService.getAllJobs();
    }

    @RequestMapping(value = "/", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public Job createJob(@RequestBody Job jobCreateRequest) {
        System.out.println("Received persistent request");
        System.out.println(jobCreateRequest);
        return this.jobService.createJob(jobCreateRequest);
    }

    @RequestMapping(value = "/{jobId}/", method = RequestMethod.GET)
    public Job getJon(@PathVariable("jobId") String jobId) {
        return this.jobService.getJobById(jobId);
    }

    @RequestMapping(value = "/{jobId}/workers/", method = RequestMethod.GET)
    public Iterable<Worker> getWorkers(@PathVariable("jobId") String jobId) {
        return workerService.getAllForJob(jobId);
    }
}
