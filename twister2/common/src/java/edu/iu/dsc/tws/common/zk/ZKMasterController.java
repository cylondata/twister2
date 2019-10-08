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
package edu.iu.dsc.tws.common.zk;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.CloseableUtils;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.JobMasterState;

public class ZKMasterController extends ZKBaseController {
  public static final Logger LOG = Logger.getLogger(ZKMasterController.class.getName());

  // Job Master IP address
  private String masterAddress;

  // persistent ephemeral znode for this worker
  private PersistentNode masterZNode;

  public ZKMasterController(Config config,
                            String jobName,
                            int numberOfWorkers,
                            String masterAddress) {
    super(config, jobName, numberOfWorkers);
    this.masterAddress = masterAddress;
  }

  /**
   * create an ephemeral znode for the job master
   * set the master address in the body of that node
   * job master status also goes into the body of that znode
   * The body of the worker znode will be updated as the status of the job master changes
   * from STARTING, RUNNING, COMPLETED
   */
  public void initialize(JobMasterState initialState) throws Exception {

    if (!(initialState == JobMasterState.JM_STARTED
        || initialState == JobMasterState.JM_RESTARTED)) {
      throw new Exception("initialState has to be either WorkerState.STARTING or "
          + "WorkerState.RESTARTING. Supplied value: " + initialState);
    }

    try {
      super.initialize();

      createJobMasterZnode(initialState);

      LOG.info("Job Master: " + masterAddress + " initialized successfully.");

    } catch (Exception e) {
      throw e;
    }
  }

  /**
   * create the znode for this worker
   */
  private void createJobMasterZnode(JobMasterState initialState) {
    String jmPath = ZKUtils.constructJobMasterPath(jobPath);

    // put masterAddress and its state into znode body
    byte[] jmZnodeBody = ZKUtils.encodeJobMasterZnode(masterAddress, initialState.getNumber());
    masterZNode = ZKUtils.createPersistentEphemeralZnode(jmPath, jmZnodeBody);
    masterZNode.start();
    try {
      masterZNode.waitForInitialCreate(10000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.log(Level.SEVERE,
          "Could not create job master znode.", e);
      throw new RuntimeException("Could not create job master znode", e);
    }

    String fullPath = masterZNode.getActualPath();
    LOG.info("An ephemeral znode is created for the job master: " + fullPath);
  }

  /**
   * Update job master status with new state
   * return true if successful
   */
  public boolean updateJobMasterStatus(JobMasterState newStatus) {

    byte[] jmZnodeBody = ZKUtils.encodeJobMasterZnode(masterAddress, newStatus.getNumber());

    try {
      client.setData().forPath(masterZNode.getActualPath(), jmZnodeBody);
      return true;
    } catch (Exception e) {
      LOG.log(Level.SEVERE,
          "Could not update job master status in znode: " + masterZNode.getActualPath(), e);
      return false;
    }
  }



  /**
   * close all local entities.
   */
  public void close() {
    try {
      CloseableUtils.closeQuietly(masterZNode);
      super.close();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception when closing", e);
    }
  }


}
