package edu.iu.dsc.tws.dashboard.services;

import edu.iu.dsc.tws.dashboard.data_models.Worker;
import edu.iu.dsc.tws.dashboard.repositories.WorkerRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class WorkerService {

    @Autowired
    private WorkerRepository workerRepository;

    public Iterable<Worker> getAllForJob(String jobId) {
        return workerRepository.findAllByJob_Id(jobId);
    }

    public Iterable<Worker> getAllWorkers() {
        return workerRepository.findAll();
    }

    public Worker createWorker(Worker worker) {
        return this.workerRepository.save(worker);
    }
}
