//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.rsched.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.api.faulttolerance.Fault;
import edu.iu.dsc.tws.api.faulttolerance.FaultAcceptable;
import edu.iu.dsc.tws.api.resource.IAllJoinedListener;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.IWorkerFailureListener;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.rsched.core.WorkerRuntime;

/**
 * Keep information about a managed environment where workers can get restarted.
 */
public class ManagedWorkerEnvironment implements IWorkerFailureListener, IAllJoinedListener {
  private static final Logger LOG = Logger.getLogger(ManagedWorkerEnvironment.class.getName());

  /**
   * Keep track of the components that have the ability to deal with faults
   */
  private List<FaultAcceptable> faultComponets = new ArrayList<>();

  /**
   * The IWorker we are working with
   */
  private IWorker managedWorker;

  /**
   * The configuration
   */
  private Config config;

  /**
   * The worker id
   */
  private int workerId;

  /**
   * The worker controller
   */
  private IWorkerController workerController;

  /**
   * Persistant volume
   */
  private IPersistentVolume persistentVolume;

  /**
   * The volatile volume
   */
  private IVolatileVolume volatileVolume;

  public ManagedWorkerEnvironment(Config config,
                                  int workerID,
                                  IWorkerController workerController,
                                  IPersistentVolume persistentVolume,
                                  IVolatileVolume volatileVolume,
                                  IWorker worker) {
    this.config = config;
    this.workerId = workerID;
    this.workerController = workerController;
    this.persistentVolume = persistentVolume;
    this.volatileVolume = volatileVolume;
    this.managedWorker = worker;

    WorkerRuntime.addWorkerFailureListener(this);
    WorkerRuntime.addAllJoinedListener(this);
  }

  /**
   * Register a fault component to this managed envrionment
   *
   * @param fa the fault accepted component
   */
  public void registerFaultComponent(FaultAcceptable fa) {
    faultComponets.add(fa);
  }

  @Override
  public void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList) {

  }

  @Override
  public void failed(int workerID) {
    // lets tell everyone that there is a fault
    for (FaultAcceptable fa : faultComponets) {
      try {
        fa.onFault(new Fault(workerID));
      } catch (Twister2Exception e) {
        LOG.log(Level.WARNING, "Cannot propergate the failure", e);
      }
    }
  }

  @Override
  public void restarted(JobMasterAPI.WorkerInfo workerInfo) {
    // wait until the previous execution is finished


    // wait until the cluster is stable


    // now lets call the execute method again
    managedWorker.execute(config, workerId, workerController, persistentVolume, volatileVolume);
  }
}
