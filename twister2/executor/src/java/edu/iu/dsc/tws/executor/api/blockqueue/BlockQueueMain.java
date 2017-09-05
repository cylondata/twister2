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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by vibhatha on 9/5/17.
 */
public class BlockQueueMain {

  public static void main(String[] args) throws InterruptedException {

    BlockingQueue q = new PriorityBlockingQueue();
    Producer p = new Producer(q);
    Consumer c1 = new Consumer(q);
    Consumer c2 = new Consumer(q);
    Thread t = new Thread(p);
    Thread t1 = new Thread(c1);
    Thread t2 = new Thread(c2);

    System.out.println("All threads started...");
    t.start();
    t1.start();
    t2.start();

    t.join();
    t1.join();
    t2.join();
    System.out.println("All threads joined...");

  }
}
