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

import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.executor.model.Task;

/**
 * Created by vibhatha on 9/5/17.
 */
public class Producer implements Runnable {

  //private final BlockingQueue queue;
  protected BlockingQueue queue = null;


  public Producer(BlockingQueue q) { queue = q; }


  public void run() {
    try {
      int i = 0;
      while(true){
        queue.put(produce("Process : "+i+" - "+"Task Descriptor : "+i));
        i++;
        Thread.sleep(100);
      }


    } catch (InterruptedException ex) {

      System.err.println(ex.getMessage());

    }
  }
  public Object produce(String taskDescriptor) throws InterruptedException {

    System.out.println("Producing : "+taskDescriptor);

    //this.queue.put(taskDescriptor);
    return new String("Task Id : "+taskDescriptor);
  }

  public int size(){
    return this.queue.size();
  }
}
