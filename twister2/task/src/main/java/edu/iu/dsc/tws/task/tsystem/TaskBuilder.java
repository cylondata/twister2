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

public class TaskBuilder {

    public TaskBuilder(){
    }

    /** This method will interface the task system or task graph **/
    /*public Task createTask(){
       return new TaskSubmitter(task);
    }*/

    /** If the task parallelism is not specified for mapper class, it will assign the default parallelism **/

    public TMap setMapper(int Id, TMap tMap){
        return setMapper (Id, tMap,0);
    }

    public TMap setMapper(int Id, TMap tMap, int taskParallelism){
        return setMapper (Id, new Mapper (tMap), taskParallelism);

    }

    /** If the task parallelism is not specified for reducer class, it will assign the default parallelism **/

    public TReduce setReducer(int Id, TReduce tReduce){
        return setReducer (Id, tReduce, 0);
    }

    public TReduce setReducer(int Id, TReduce tReduce, int taskParallelism){
        return setReducer(Id, new Reducer(tReduce), taskParallelism);
    }

}
