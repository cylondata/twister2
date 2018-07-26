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
package edu.iu.dsc.tws.rsched.bootstrap;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentNode;

import edu.iu.dsc.tws.common.config.Config;

/**
 * it is a single node controller
 * A single node registers, others discover that node
 * In our case, Job Master registers, workers discover the job master
 * Job Master creates an ephemeral, persistent znode
 * Job Master IP and port number is put as the payload to this node in the form of: <ip>:<port>
 * the node will be automatically deleted once this class disconnects from ZooKeeper server
 */
public class ZKJobMasterRegistrar {
  private static final Logger LOG = Logger.getLogger(ZKJobMasterRegistrar.class.getName());

  private String jobMasterIP; // hostname and port number of JobMaster
  private int jobMasterPort; // hostname and port number of JobMaster
  private Config config;

  private CuratorFramework client;
  private String jobMasterPath;
  private PersistentNode jobMasterNode;

  public ZKJobMasterRegistrar(Config config, String jobMasterIP, int jobMasterPort) {
    this.config = config;
    this.jobMasterIP = jobMasterIP;
    this.jobMasterPort = jobMasterPort;
    jobMasterPath = ZKUtil.constructJobMasterPath(config);
  }

  /**
   * connect to ZooKeeper server
   * @return
   */
  public boolean initialize() {
    // connect to ZooKeeper server if it is not already connected
    if (client == null) {
      client = ZKUtil.connectToServer(config);
    }

    // first check whether there is already a node with the same name
    if (sameZNodeExist()) {
      LOG.severe("Same znode exist. Could not initialize JobMasterRegistrar.");
      return false;
    }

    boolean znodeCreated = createJobMasterZnode();
    if (znodeCreated) {
      LOG.info("JobMasterRegistrar initialized successfully");
    } else {
      LOG.info("JobMasterRegistrar could not be initialized successfully");
    }

    return znodeCreated;
  }

  /**
   * check whether there is already the same node on ZooKeeper server
   * this can happen when the JobMasterRegistrar is not properly closed and
   * it is started again immediately.
   * ZooKeeper takes around 30 seconds to delete ephemeral znodes in those cases
   * During this time, if JobMasterRegistrar restarts with the same job name,
   * this can happen
   * @return
   */
  public boolean sameZNodeExist() {
    if (client == null) {
      ZKUtil.connectToServer(config);
    }

    try {
      return client.checkExists().forPath(jobMasterPath) != null;

    } catch (Exception e) {
      LOG.log(Level.SEVERE,
          "Exception when trying to check the existence of the znode: " + jobMasterPath, e);
      return false;
    }
  }

  /**
   * create the znode for the job master
   */
  private boolean createJobMasterZnode() {
    String jobMasterIPandPort = jobMasterIP + ":" + jobMasterPort;
    try {
      jobMasterNode = ZKUtil.createPersistentZnode(
          client, jobMasterPath, jobMasterIPandPort.getBytes());
      jobMasterNode.start();
      jobMasterNode.waitForInitialCreate(10000, TimeUnit.MILLISECONDS);
      jobMasterPath = jobMasterNode.getActualPath();
      LOG.info("An ephemeral znode is created for the Job Master: " + jobMasterPath);
      return true;
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not create znode for the Job Master: " + jobMasterIPandPort, e);
      return false;
    }
  }

  /**
   * this method can be used to delete the znode from a previous session
   * if you don't want to wait the ZooKeeper to delete the ephemeral znode
   * if ZKJobMasterRegistrar is not closed properly in the previous session,
   * it takes around 30 seconds for ZooKeeper to delete the ephemeral JobMaster node
   */
  public void deleteJobMasterZNode() {
    if (client == null) {
      ZKUtil.connectToServer(config);
    }

    try {
      client.delete().forPath(jobMasterPath);
      LOG.info("Previously existing Znode deleted: " + jobMasterPath);
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Exception when deleting the previous Znode: " + jobMasterPath, e);
    }
  }

  public void close() {

    if (jobMasterNode != null) {
      try {
        jobMasterNode.close();
      } catch (IOException e) {
        LOG.log(Level.WARNING, "Exception when deleting Job Master Znode: " + jobMasterPath, e);
      }
    }

    client.close();
  }

}
