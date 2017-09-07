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

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.executor.model.Task;

/**
 * Created by vibhatha on 9/5/17.
 */
public class Consumer implements Runnable {
  //private final BlockingQueue queue;
  protected BlockingQueue queue = null;
  public Consumer(BlockingQueue q) { queue = q; }

  public void run() {
    try {
      boolean status = true;
      System.out.println("Initial Queue Size : "+queue.size());
        while(status){
          consume((String) queue.take());
         /* if(queue.size()==0){
            break;
          }*/
        }

    } catch (InterruptedException ex) {
      ex.printStackTrace();
    }
  }
  public void consume(String taskDescriptor) {
    System.out.println("Consuming : "+taskDescriptor.toString());

  }
}
