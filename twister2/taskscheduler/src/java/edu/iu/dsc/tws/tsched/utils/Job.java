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
package edu.iu.dsc.tws.tsched.utils;

@SuppressWarnings("ALL")
public class Job {

  public final int Number_Of_Containers = 2;
  public final int Number_Of_Instances = 4;
  public int Total_Number_Of_Instances = 3;
  public int jobId = 1;

  public Job(){

  }

  public Task[] getTaskList() {

    return null;
  }

  public int getId() {
    return jobId;
  }

  public class Task {

    private String taskName = "mpitask";
    private Integer taskCount = 5;

    public String getTaskName() {
       return taskName;
    }

    public Integer getParallelTaskCount() {
        return taskCount;
    }
  }
}
