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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.iu.dsc.tws.executor.model.Task;

/**
 * Created by vibhatha on 8/25/17.
 */
public class TaskHandler {

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
  protected final Lock readLock = readWriteLock.readLock();
  protected final Lock writeLock = readWriteLock.writeLock();


  private List<Task> tasks = new ArrayList<Task>();
  private List<ITaskAddedListener> taskAddedListeners = new ArrayList<>();

  public void addTask (Task task){
    this.tasks.add(task);
    this.notifyTaskAddedListener(task);
  }

  public synchronized ITaskAddedListener registerTaskAddedListener (ITaskAddedListener iTaskAddedListener){
    // Lock the list of listeners for writing

    this.writeLock.lock();
    try {
      this.taskAddedListeners.add(iTaskAddedListener);
    }catch (Exception ex){
      System.out.println(ex.getMessage());

    }finally{
      this.writeLock.unlock();
    }
    return iTaskAddedListener;
  }

  public synchronized void unregisterTasAddedListener(ITaskAddedListener iTaskAddedListener){

    // Lock the list of listeners for writing

    this.writeLock.lock();
    try{
      this.taskAddedListeners.remove(iTaskAddedListener);
    }catch (Exception ex){
      System.out.println(ex.getMessage());

    }finally {
      this.writeLock.unlock();
    }


  }

  public synchronized void notifyTaskAddedListener(Task task){

    this.writeLock.lock();
    try{
      //this.taskAddedListeners.forEach(taskAddedListener -> taskAddedListener.updateTaskAdded(task));
    for (ITaskAddedListener iTaskAddedListener : taskAddedListeners) {
      iTaskAddedListener.updateTaskAdded(task);
    }

    }catch (Exception ex){
      System.out.println(ex.getMessage());

    }finally {
      this.writeLock.unlock();
    }

  }

}
