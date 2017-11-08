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

import edu.iu.dsc.tws.task.tsystem.*;

public class TaskClient {

    private TaskClient() {
    }

    public static void main(String[] args) {

        if(args.length !=1){
            throw new RuntimeException ("Specify the job type -> Batch or Streaming or MPI or FaaS");
        }

        if(args.length >=1 && args[0].equals ("MPI")){
            throw new RuntimeException ("Specify the number of parallel tasks to run");
        }

        TaskBuilder taskBuilder = new TaskBuilder ();

        taskBuilder.setMapper(0, new ClientMap(), 2);
        taskBuilder.setReducer(0, new ClientReduce(), 2);

        TaskConfiguration taskConfiguration = new TaskConfiguration ();
        taskConfiguration.setConfiguration("/home/kgovind/twister2/task.conf");

        /** For setting the Required Ram, Disk, and CPU values here **/
        taskConfiguration.setTaskRequiredCPU (20);
        taskConfiguration.setTaskRequiredMemory (200);
        taskConfiguration.setTaskRequiredCPU (5);

        /** Pass the task and task configuration objects into the task submitter for initiating the execution of task **/
        TaskSubmitter taskSubmitter = new TaskSubmitter(args[0], taskBuilder, taskConfiguration);
        taskSubmitter.execute();

        System.out.println("Task Submitted to the Task System");
    }

    /** This is sample class which implements the task system TMap interface ***/
    public static class ClientMap implements TMap {

        public ClientMap(){
        }

        @Override
        public void map(Object value, MapOutputCollector out) {
            String[] tokens = value.toString ().toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new DataTuple<String, Integer>(token, 1));
                }
            }
        }
    }

    /** This is sample class which implements the task system TReduce interface ***/
    public static class ClientReduce implements TReduce {

        public ClientReduce(){
        }

        @Override
        public void reduce(Object value, ReduceOutputCollector out) {
            String[] tokens = value.toString ().toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new DataTuple<String, Integer>(token, 1));
                }
            }
        }

    }
}
