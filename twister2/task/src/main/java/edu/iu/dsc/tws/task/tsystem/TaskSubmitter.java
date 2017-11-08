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
package edu.iu.dsc.tws.task.tsystem;

public class TaskSubmitter {

    private final String taskType;
    private final TaskBuilder taskBuilder;
    private final TaskConfiguration taskConfiguration;

    public TaskSubmitter(String taskType, TaskBuilder taskBuilder, TaskConfiguration taskConfiguration){

            this.taskType = taskType;
            this.taskBuilder = taskBuilder;
            this.taskConfiguration = taskConfiguration;
    }

    public void execute(){
        System.out.println("Task type is:"+this.taskType+"\t"+"task builder:"+this.taskBuilder+"\t"+
                                                    "task configuration:"+this.taskConfiguration);
    }
}
