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
package edu.iu.dsc.tws.master.driver;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.common.driver.IScaler;
import edu.iu.dsc.tws.common.driver.IScalerPerCluster;
import edu.iu.dsc.tws.common.driver.NullScalar;
import edu.iu.dsc.tws.master.server.WorkerMonitor;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public class Scaler implements IScaler {

  private static final Logger LOG = Logger.getLogger(Scaler.class.getName());

  private IScalerPerCluster clusterScaler;
  private WorkerMonitor workerMonitor;
  private ZKJobUpdater zkJobUpdater;
  private JobAPI.Job job;

  public Scaler(JobAPI.Job job,
                IScalerPerCluster clusterScaler,
                WorkerMonitor workerMonitor,
                ZKJobUpdater zkJobUpdater) {

    this.job = job;
    this.workerMonitor = workerMonitor;
    this.clusterScaler = clusterScaler;
    this.zkJobUpdater = zkJobUpdater;

    if (this.clusterScaler == null) {
      this.clusterScaler = new NullScalar();
    }
  }

  @Override
  public boolean isScalable() {
    return clusterScaler.isScalable();
  }

  @Override
  public boolean scaleUpWorkers(int instancesToAdd) {
    if (!isScalable()) {
      LOG.severe("Job is not scalable. Either ComputeResource is not scalable or "
          + "this is an OpenMPI job.");
      return false;
    }

    if (instancesToAdd <= 0) {
      LOG.severe("instancesToAdd has to be a positive integer");
      return false;
    }

    boolean scaledUp = clusterScaler.scaleUpWorkers(instancesToAdd);
    if (!scaledUp) {
      return false;
    }

    workerMonitor.workersScaledUp(instancesToAdd);

    // calculate numberOfWorkers in the job
    int numberOfWorkers = job.getNumberOfWorkers() + instancesToAdd;
    return updateJobInZK(numberOfWorkers);
  }

  @Override
  public boolean scaleDownWorkers(int instancesToRemove) {
    if (!isScalable()) {
      LOG.severe("Job is not scalable. Either ComputeResource is not scalable or "
          + "this is an OpenMPI job.");
      return false;
    }

    if (instancesToRemove <= 0) {
      LOG.severe("instancesToRemove has to be a positive integer");
      return false;
    }

    boolean scaledDown = clusterScaler.scaleDownWorkers(instancesToRemove);
    if (!scaledDown) {
      return false;
    }

    workerMonitor.workersScaledDown(instancesToRemove);

    // calculate numberOfWorkers in the job
    int numberOfWorkers = job.getNumberOfWorkers() - instancesToRemove;
    return updateJobInZK(numberOfWorkers);
  }

  private boolean updateJobInZK(int numberOfWorkers) {

    // update the job object
    job = job.toBuilder().setNumberOfWorkers(numberOfWorkers).build();

    // update the job in ZooKeeper if it is used
    try {
      zkJobUpdater.updateJob(job);
      return true;
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return false;
    }
  }
}
