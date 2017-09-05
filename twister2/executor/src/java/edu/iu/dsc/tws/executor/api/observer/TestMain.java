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
package edu.iu.dsc.tws.executor.api.observer;

import edu.iu.dsc.tws.executor.test.TaskManager;

/**
 * Created by vibhatha on 8/25/17.
 */
public class TestMain {
  public static void main(String[] args) throws InterruptedException {
    TaskHandler taskHandler = new TaskHandler();
    taskHandler.registerTaskAddedListener(new GetInfoTaskAddedListener());
    taskHandler.registerTaskAddedListener(new GetCountTaskAddedListener());

    String thread1Name = "1";
    Runnable taskManager1 = new TaskManager(taskHandler, thread1Name);
    Thread thread1 = new Thread(taskManager1);
    thread1.setDaemon(true);
    thread1.setName("taskManager1");



    String thread2Name = "2";
    Runnable taskManager2= new TaskManager(taskHandler, thread2Name);
    Thread thread2 = new Thread(taskManager2);
    thread2.setDaemon(true);
    thread2.setName("taskManager2");

    thread1.start();
    thread2.start();
    thread1.join();
    thread2.join();




  }
}
