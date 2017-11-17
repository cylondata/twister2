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
package edu.iu.dsc.tws.task.taskgraphbuilder;

import edu.iu.dsc.tws.task.taskgraphbuilder.DirectedTaskGraph;
import edu.iu.dsc.tws.task.taskgraphbuilder.TaskGraph;

import java.util.Set;

public class Main {

    public static void main(String[] args){

        DefaultDirectedTaskGraph<Mapper, CManager> taskVertices = new DefaultDirectedTaskGraph<Mapper, CManager> (CManager.class);

        Mapper task1 = new Mapper ("1", 1) {
            @Override
            public void execute() {

            }

            @Override
            public void execute(Mapper mapper) {

            }
        };

        Mapper task2 = new Mapper ("2", 2) {
            @Override
            public void execute() {

            }

            @Override
            public void execute(Mapper mapper) {

            }
        };

        taskVertices.addTaskEdge (task1, task2);

        System.out.println("Task Vertices are:"+taskVertices.toString ());

    }
}
