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

import edu.iu.dsc.tws.dashboard.data_models.Job;
import edu.iu.dsc.tws.dashboard.data_models.JobState;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import java.util.Date;
import java.util.List;

public interface JobRepository extends CrudRepository<Job, String> {

  @Modifying
  @Query("update Job job set job.state=?2 where job.id=?1")
  int changeJobState(String jobId, JobState jobState);

  @Modifying
  @Query("update Job job set job.heartbeatTime=?2 where job.id=?1")
  int heartbeat(String jobId, Date now);

  @Query("select job.state, count(job.state) from Job job group by job.state")
  List<Object[]> getStateStats();
}
