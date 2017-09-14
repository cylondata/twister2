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
package edu.iu.dsc.tws.executor.api.blockqueue;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import edu.iu.dsc.tws.executor.constants.TaskOps;
import edu.iu.dsc.tws.executor.model.Task;

/**
 * Created by vibhatha on 9/5/17.
 */
public class BlockQueueMain {

  public static void main(String[] args) throws InterruptedException {

    BlockingQueue q1 = new PriorityBlockingQueue();
    BlockingQueue q = new ArrayBlockingQueue(5);

    Task task1 = new Task(1001,"Task 1",new Date(), "scripts/task_scripts/run-task1.sh", "Thread 1");
    Task task2 = new Task(1002,"Task 2",new Date(), "scripts/task_scripts/run-task2.sh", "Thread 1");
    Task task3 = new Task(1003,"Task 3",new Date(), "scripts/task_scripts/run-task3.sh", "Thread 1");
    Task task4 = new Task(1004,"Task 4",new Date(), "scripts/task_scripts/run-task4.sh", "Thread 1");
    Task task5 = new Task(1005,"Task 5",new Date(), "scripts/task_scripts/run-task5.sh", "Thread 1");
    ArrayList<Task> taskArrayList = new ArrayList<>();
    taskArrayList.add(task1);
    taskArrayList.add(task2);
    taskArrayList.add(task3);
    taskArrayList.add(task4);
    taskArrayList.add(task5);

    // Demo Pub/Sup BlockingQueue

    /*Producer p = new Producer(q);
    Consumer c1 = new Consumer(q);
    Consumer c2 = new Consumer(q);
    Thread t = new Thread(p);
    t.start();

    Thread t1 = new Thread(c1);
    t1.start();


    System.out.println("Consumer : "+t1.getState());
    System.out.println("Producer : "+t.getState());
*/
    // Producing and Consuming real resources
    /*Producer p = new Producer(q, task1);
    Consumer c1 = new Consumer(q);
    Consumer c2 = new Consumer(q);
    Thread t = new Thread(p);
    t.start();

    Thread t1 = new Thread(c1);
    t1.start();


    System.out.println("Consumer : "+t1.getState());
    System.out.println("Producer : "+t.getState());
*/
    //consuming list of tasks

    StaticProducer p = new StaticProducer(q, taskArrayList, TaskOps.LIST);
    p.create();
    q = p.getQueue();
    System.out.println("Generated Queue : "+q.size());

    Consumer c1 = new Consumer(q, TaskOps.LIST);
    //Consumer c2 = new Consumer(q, TaskOps.LIST);


    Thread t1 = new Thread(c1);
    t1.start();


    System.out.println("Consumer : "+t1.getState());



  }
}
