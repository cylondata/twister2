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
package edu.iu.dsc.tws.rsched.schedulers.standalone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.discovery.NodeInfoUtils;
import edu.iu.dsc.tws.common.discovery.WorkerInfoUtils;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class MPIWorkerController implements IWorkerController {
  private int thisWorkerID;

  private Map<Integer, JobMasterAPI.WorkerInfo> networkInfoMap = new HashMap<>();

  public MPIWorkerController(int thisWorkerID, Map<Integer, String> processNames) {
    this.thisWorkerID = thisWorkerID;
    for (Map.Entry<Integer, String> e : processNames.entrySet()) {
      JobMasterAPI.NodeInfo nodeInfo = NodeInfoUtils.createNodeInfo(e.getValue(), null, null);
      JobMasterAPI.WorkerInfo workerInfo =
          WorkerInfoUtils.createWorkerInfo(e.getKey(), e.getValue(), 0, nodeInfo);
      networkInfoMap.put(e.getKey(), workerInfo);
    }
  }

  @Override
  public JobMasterAPI.WorkerInfo getWorkerInfo() {
    return networkInfoMap.get(thisWorkerID);
  }

  @Override
  public JobMasterAPI.WorkerInfo getWorkerInfoForID(int id) {
    return networkInfoMap.get(id);
  }

  @Override
  public int getNumberOfWorkers() {
    return networkInfoMap.size();
  }

  @Override
  public List<JobMasterAPI.WorkerInfo> getWorkerList() {
    return new ArrayList<>(networkInfoMap.values());
  }

  @Override
  public List<JobMasterAPI.WorkerInfo> waitForAllWorkersToJoin(long timeLimitMilliSec) {
    return new ArrayList<>(networkInfoMap.values());
  }

  @Override
  public boolean waitOnBarrier(long timeLimitMilliSec) {
    return false;
  }
}
