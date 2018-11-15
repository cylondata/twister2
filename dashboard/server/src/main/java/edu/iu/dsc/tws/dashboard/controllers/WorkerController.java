package edu.iu.dsc.tws.dashboard.controllers;

import edu.iu.dsc.tws.dashboard.data_models.Worker;
import edu.iu.dsc.tws.dashboard.services.WorkerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WorkerController {

    @Autowired
    private WorkerService workerService;

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public Iterable<Worker> getAllWorkers() {
        return workerService.getAllWorkers();
    }
}
