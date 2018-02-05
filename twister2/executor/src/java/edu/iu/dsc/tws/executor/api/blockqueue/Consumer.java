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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.executor.constants.TaskOps;
import edu.iu.dsc.tws.executor.model.Task;

/**
 * Created by vibhatha on 9/5/17.
 */
public class Consumer implements Runnable {
  //private final BlockingQueue queue;
  protected BlockingQueue queue = null;
  protected TaskOps executableType= TaskOps.SINGLE;

  public Consumer(BlockingQueue q) { queue = q; }

  public Consumer(BlockingQueue queue, TaskOps executableType) {
    this.queue = queue;
    this.executableType = executableType;
  }

  public void run() {
    try {
      boolean status = true;
      System.out.println("Initial Queue Size : "+queue.size());

      if(executableType == TaskOps.SINGLE){
        LOGOUT("Single Task Operations");
        consumeTask((Task)queue.take());

      }

      if(executableType == TaskOps.LIST){
        LOGOUT("Task List Operations with Queue size :"+queue.size());
        int size = queue.size();
        for (int i = 0; i < size; i++) {
          consumeTask((Task)queue.take());
          LOGOUT("Current Queue Size : "+queue.size());
        }


      }

      if(executableType == TaskOps.CONTINUES){

        while(status){
          LOGOUT("Continuous Task Operations");
          //consume((String) queue.take());
          consumeTask((Task)queue.take());
         /* if(queue.size()==0){
            break;
          }*/
        }

      }



    } catch (InterruptedException ex) {
      ex.printStackTrace();
    }
  }
  public void consume(String taskDescriptor) {
    System.out.println("Consuming : "+taskDescriptor.toString());

  }

  public void consumeTask(Task task){

    try {
      System.out.println("Consuming Task : "+task.getName()+ " , "+task.getDescription());
      execute(task);
    } catch (IOException e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
  }

  public void execute(Task task) throws IOException {
    /*ProcessBuilder pb = new ProcessBuilder(task.getDescription());
    Map<String, String> env = pb.environment();
    env.put("VAR1", "myValue");
    env.remove("OTHERVAR");
    env.put("VAR2", env.get("VAR1") + "suffix");
    pb.directory(new File("myDir"));
    Process p = pb.start();*/
    Process process = Runtime.getRuntime().exec(task.getDescription());
    boolean status = process.isAlive();
    LOGOUT("Status : "+status);

    /*int exitValue = process.exitValue();
    LOGOUT("==============================");
    LOGOUT("Exit Value : "+exitValue);
    LOGOUT(process.getOutputStream().toString());
    LOGOUT("==============================");*/
  }

  public void LOGOUT(String message){
    System.out.println(message);
  }

}
