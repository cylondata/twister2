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
package edu.iu.dsc.tws.api.cdfw;

import java.util.List;
import java.util.logging.Logger;

import com.google.protobuf.Any;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.driver.IDriverMessenger;
import edu.iu.dsc.tws.common.driver.IScaler;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class CDFWEnv {
  private static final Logger LOG = Logger.getLogger(CDFWEnv.class.getName());

  private CDFWExecutor cdfwExecutor;

  private IScaler resourceScaler;

  private Config config;

  // volatile because workerInfoList should be the same for all the threads
  private volatile List<JobMasterAPI.WorkerInfo> workerInfoList;

  public CDFWEnv(Config config, IScaler resourceScaler, IDriverMessenger driverMessenger,
                 List<JobMasterAPI.WorkerInfo> workers) {
    this.resourceScaler = resourceScaler;
    this.config = config;
    this.workerInfoList = workers;
    this.cdfwExecutor = new CDFWExecutor(this, driverMessenger);
  }

  public Config getConfig() {
    return config;
  }

  public void executeDataFlowGraph(DataFlowGraph dataFlowGraph) {
    this.cdfwExecutor.execute(dataFlowGraph);
  }

  public void increaseWorkers(int workers) {
    this.resourceScaler.scaleUpWorkers(workers);
  }

  public void decreaseWorkers(int workers) {
    this.resourceScaler.scaleDownWorkers(workers);
  }

  public List<JobMasterAPI.WorkerInfo> getWorkerInfoList() {
    // this method could be problematic, if the worker list changes in the middle of the execution
    return workerInfoList;
  }

  public void workerMessageReceived(Any anyMessage, int senderWorkerID) {
    this.cdfwExecutor.workerMessageReceived(anyMessage, senderWorkerID);
  }

  public void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList) {
    // this is the only method which could change the
    // might have to put these messages into a queue to accommodate scaling up/ down workers

    // when all workers joined, update the worker info list
    this.workerInfoList = workerList;
  }

  public void close() {
    this.cdfwExecutor.close();
  }

}
