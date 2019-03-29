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
package edu.iu.dsc.tws.dashboard.repositories;

import java.util.Date;
import java.util.List;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import edu.iu.dsc.tws.dashboard.data_models.Worker;
import edu.iu.dsc.tws.dashboard.data_models.WorkerState;
import edu.iu.dsc.tws.dashboard.data_models.composite_ids.WorkerId;

public interface WorkerRepository extends CrudRepository<Worker, WorkerId> {

  Iterable<Worker> findAllByJob_JobID(String jobId);

  @Modifying
  @Query("update Worker worker set worker.state=?3 "
          + "where worker.job.jobID=?1 and worker.workerID=?2")
  int changeWorkerState(String jobId, Long workerId, WorkerState workerState);

  @Modifying
  @Query("update Worker worker set worker.state=?2 "
          + "where worker.job.jobID=?1 and worker.state<>?3")
  int changeStateOfAllWorkers(String jobId, WorkerState workerState, WorkerState excludeState);

  @Modifying
  @Query("update Worker worker set worker.heartbeatTime=?3 "
          + "where worker.job.jobID=?1 and worker.workerID=?2")
  int heartbeat(String jobId, Long workerId, Date now);

  @Query("select worker.state, count(worker.state) from Worker worker group by worker.state")
  List<Object[]> getStateStats();
}
