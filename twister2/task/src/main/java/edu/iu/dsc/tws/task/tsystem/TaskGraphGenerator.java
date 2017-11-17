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

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.alg.CycleDetector;
import org.jgrapht.graph.DefaultEdge;

public class TaskGraphGenerator extends TaskGraph{

    public TaskGraphGenerator (){

        TaskGraph taskGraph = new TaskGraph ();

        AbstractMapper task1 = new Task1("1",1);

        AbstractMapper task2 = new Task2("2",2);

        AbstractMapper task3 = new Task3("3", 5);

        AbstractMapper task4 = new Task1("4", 4);

        //Sample TaskGraphs

        //Task Graph 1
        /*this.generateTaskGraph (task1)
            .generateTaskGraph (task2, task1)
            .generateTaskGraph (task3, task2, task1);*/

        //Task Graph 2
        /*this.generateTaskGraph (task1)
                .generateTaskGraph (task2, task1)
                .generateTaskGraph (task3, task1)
                .generateTaskGraph (task4, task2, task3, task1);*/

        //Task Graph 3
        /*this.generateTaskGraph (task1)
                .generateTaskGraph (task2, task1)
                .generateTaskGraph (task3, task1);*/

        //Task Graph 3
        /*this.generateTaskGraph (task1)
                .generateTaskGraph (task2)
                .generateTaskGraph (task3);*/

        System.out.println("--------------------------------------------------------------");
        System.out.println("\t\tGenerated Total Tasks:"+TaskGraph.totalNumberOfTasks);
        System.out.println("--------------------------------------------------------------");

        try {

            DirectedGraph<AbstractMapper, CManager> tVertices = new DefaultDirectedGraph<AbstractMapper, CManager> (CManager.class);

            AbstractMapper taskx = new Task1("1",1);
            AbstractMapper tasky = new Task2("2",2);

            tVertices.addVertex(taskx);
            tVertices.addVertex(tasky);

            //CManager taskX = new CManager ("x", 1);
            //CManager taskY = new CManager ("x", 3);
            CManager taskX = new CManager ("send");
            tVertices.addEdge (taskx, tasky, taskX);
            System.out.println ("Generated DataFlow Graph:"+tVertices);
            System.out.println("--------------------------------------------------------------");

            /*Mapper task5 = new CManager ("1", 10);
            Mapper task6 = new CManager ("1", 10);*/
            //this.generateDataflowGraph (taskx)
            this.generateDataflowGraph (taskx, tasky, taskX);

        }
        catch (Exception ee){
            ee.printStackTrace ();
        }
    }

    public static void main(String[] args) throws Exception{

        TaskGraph taskGraph = new TaskGraphGenerator ();
        System.out.println("Generated Task Graph:"+taskGraph);

        TaskGraphParser taskGraphParser = new TaskGraphParser (taskGraph);
        taskGraphParser.taskGraphParseAndSchedule ();

        //For testing generate the task 'x' and submit it to the Executor along with Communication Manager 'COMMUNICATE'
        /*Mapper mapperTask = new Task1("x",1);
        CommunicationManager communicationManager = new CommunicationManager ("COMMUNICATE");

        Executor executor = new Executor ();
        executor.execute (mapperTask, CommunicationManager.class);*/


        AbstractMapper Task1 = new Task1("x", 1);
        AbstractMapper Task2 = new Task2("y",2);

        Executor executor = new Executor ();
        executor.execute (Task1, Task2, new CManager("Connect Task1 and Task2"));

    }
}
