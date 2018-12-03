package edu.iu.dsc.tws.dashboard.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import edu.iu.dsc.tws.dashboard.data_models.ComputeResource;
import edu.iu.dsc.tws.dashboard.data_models.Job;
import edu.iu.dsc.tws.dashboard.data_models.Node;
import edu.iu.dsc.tws.dashboard.data_models.Worker;
import edu.iu.dsc.tws.dashboard.data_models.WorkerState;
import edu.iu.dsc.tws.dashboard.repositories.WorkerRepository;
import edu.iu.dsc.tws.dashboard.rest_models.StateChangeRequest;
import edu.iu.dsc.tws.dashboard.rest_models.WorkerCreateRequest;

import javax.persistence.EntityNotFoundException;

@Service
public class WorkerService {

  private final WorkerRepository workerRepository;

  private final JobService jobService;

  private final NodeService nodeService;

  private final ComputeResourceService computeResourceService;

  @Autowired
  public WorkerService(WorkerRepository workerRepository, JobService jobService, NodeService nodeService, ComputeResourceService computeResourceService) {
    this.workerRepository = workerRepository;
    this.jobService = jobService;
    this.nodeService = nodeService;
    this.computeResourceService = computeResourceService;
  }

  public Iterable<Worker> getAllForJob(String jobId) {
    return workerRepository.findAllByJob_JobId(jobId);
  }

  public Iterable<Worker> getAllWorkers() {
    return workerRepository.findAll();
  }

  public Worker createWorker(WorkerCreateRequest workerCreateRequest) {
    Worker worker = new Worker();
    worker.setWorkerId(workerCreateRequest.getWorkerId());
    worker.setWorkerIP(workerCreateRequest.getWorkerIP());
    worker.setWorkerPort(workerCreateRequest.getWorkerPort());

    //job
    Job jobById = jobService.getJobById(workerCreateRequest.getJobId());
    worker.setJob(jobById);

    //node
    Node node = nodeService.createNode(workerCreateRequest.getNode());
    worker.setNode(node);

    //compute resource
    ComputeResource computeResource = computeResourceService.findById(
            workerCreateRequest.getJobId(),
            workerCreateRequest.getComputeResourceIndex()
    );
    worker.setComputeResource(computeResource);


    return this.workerRepository.save(worker);
  }

  @Transactional
  public void changeState(String jobId, Long workerId, StateChangeRequest<WorkerState> stateChangeRequest) {
    int amountChanged = this.workerRepository.changeWorkerState(jobId, workerId, stateChangeRequest.getState());
    if (amountChanged == 0) {
      throw new EntityNotFoundException("No such worker " + workerId
              + " found in job " + jobId);
    }
  }
}
