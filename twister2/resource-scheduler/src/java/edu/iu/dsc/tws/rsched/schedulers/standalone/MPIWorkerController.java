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

import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.exceptions.TimeoutException;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class MPIWorkerController implements IWorkerController {
  private int thisWorkerID;

  private Map<Integer, JobMasterAPI.WorkerInfo> networkInfoMap = new HashMap<>();

  private Map<String, Object> runtimeObjects = new HashMap<>();

  public MPIWorkerController(int thisWorkerID, Map<Integer, JobMasterAPI.WorkerInfo> processNames) {
    this.thisWorkerID = thisWorkerID;
    this.networkInfoMap = processNames;
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
  public List<JobMasterAPI.WorkerInfo> getJoinedWorkers() {
    return new ArrayList<>(networkInfoMap.values());
  }

  @Override
  public List<JobMasterAPI.WorkerInfo> getAllWorkers() throws TimeoutException {
    return new ArrayList<>(networkInfoMap.values());
  }

  @Override
  public void waitOnBarrier() throws TimeoutException {
  }

  public void add(String name, Object obj) {
    runtimeObjects.put(name, obj);
  }

  @Override
  public Object getRuntimeObject(String name) {
    return runtimeObjects.get(name);
  }
}
