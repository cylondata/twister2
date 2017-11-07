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
package edu.iu.dsc.tws.task.tsystem;

import edu.iu.dsc.tws.api.basic.taskinformation.TaskInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class Task implements Runnable, TaskInterface {

    private static final Logger log = LoggerFactory.getLogger (Task.class);

    /** Job ID belongs to the Task **/
    //private final JobId jobId;

    /** Execution ID of the task **/
    //private final ExecutorID executorId;

    /**Task ID of the task 't' **/
    private final String taskId;
    //private final Integer taskId;

    /** The execution thread variable **/
    private final Thread executionThread;

    /**The task information such as ----------- required for the task 't' **/
    //private final TaskInformation taskInformation;

    /**The configuration values required for the task 't' **/
    //private final TaskConfiguration taskConfiguration;

    /**The jar files required to execute the task 't'**/
    //private final Collection<String> taskRequiredJarFiles;

    /**The input files required to execute the task 't' **/
    //private final Collection<String> taskRequiredInputFiles;

    /**The classpaths required to execute the task 't' **/
    //private final Collection<String> taskClassPaths;


    /** It will be enabled once we have task information and task configuration objects **/

    /*public Task(String taskId, TaskInformation taskInformation,
                TaskConfiguration taskConfiguration, Collection<String> requiredJarFiles,
                Collection<String> requiredInputFiles, Collection<String> requiredClasspaths) {

        this.taskId = taskId;
        this.taskInformation = taskInformation;
        this.taskConfiguration = taskConfiguration;
        this.taskRequiredJarFiles = requiredJarFiles;
        this.taskRequiredInputFiles = requiredInputFiles;

        executionThread = new Thread("Twister2 Task Thread");

    }*/

    public Task(String taskId, String threadName){
        this.taskId = taskId;
        executionThread = new Thread(threadName);
    }

    public String getTaskId() {
        return taskId;
    }

    /*public TaskInformation getTaskInformation() {
        return taskInformation;
    }

    public TaskConfiguration getTaskConfiguration() {
        return taskConfiguration;
    }

    public Collection<String> getTaskRequiredJarFiles() {
        return taskRequiredJarFiles;
    }

    public Collection<String> getTaskRequiredInputFiles() {
        return taskRequiredInputFiles;
    }*/

    public Thread getExecutionThread() {
        return executionThread;
    }

    @Override
    public void run() {

        while(true){
            System.out.println("I am executing my task thread");
        }
    }

    /** Implement this method for actual execution **/
    public void execute(){
        executionThread.start();
    }

    /** This method should be invoked when the task is ready for execution and it is not running previously **/
    // check the other conditions to
    private void dispatcher(Runnable runnable){
        synchronized (this){
            //create an executor object and submit it to the executor service
            try{

            }
            catch(Exception ee){
                ee.printStackTrace();
            }
        }
    }

    @Override
    public void cancel(){

    }

    public void createTask() {

    }

    public static void main(String[] args) {
        Task task = new Task("1", "execution thread");
        Thread t1 = new Thread(task);
        t1.start ();
        //task.execute ();
    }

}
