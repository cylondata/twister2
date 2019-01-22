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

import com.google.protobuf.Any;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.driver.IDriverMessenger;
import edu.iu.dsc.tws.common.driver.IScaler;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class CDFWEnv {

  private CDFWExecutor cdfwExecutor;

  private IScaler resourceScaler;

  private Config config;

  private List<JobMasterAPI.WorkerInfo> workerInfoList;


  public CDFWEnv(Config config, IScaler resourceScaler, IDriverMessenger driverMessenger) {
    this.resourceScaler = resourceScaler;
    this.config = config;
    this.cdfwExecutor = new CDFWExecutor(config, driverMessenger);
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
    return workerInfoList;
  }

  public void setWorkerInfoList(List<JobMasterAPI.WorkerInfo> workerInfoList) {
    this.workerInfoList = workerInfoList;
    this.cdfwExecutor.setWorkerList(workerInfoList);
  }

  public void workerMessageReceived(Any anyMessage, int senderWorkerID) {
    this.cdfwExecutor.workerMessageReceived(anyMessage, senderWorkerID);
  }

  public void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList) {
    setWorkerInfoList(workerList);
  }

  public void close(){
    this.cdfwExecutor.close();
  }

}
