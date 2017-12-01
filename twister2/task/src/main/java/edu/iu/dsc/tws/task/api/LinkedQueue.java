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
package edu.iu.dsc.tws.task.api;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Queue implementation based on the Java LinkedQueue
 */
public class LinkedQueue<T> implements Queue<T> {
  private LinkedBlockingQueue<T> queue;

  public LinkedQueue() {
    queue = new LinkedBlockingQueue<T>();
  }

  @Override
  public boolean add(T e) {
    return queue.add(e);
  }

  @Override
  public T remove() {
    return queue.remove();
  }

  @Override
  public T peek() {
    return queue.peek();
  }

  @Override
  public T poll() {
    return queue.poll();
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public int size() {
    return queue.size();
  }

  public LinkedBlockingQueue<T> getQueue() {
    return queue;
  }

  public void setQueue(LinkedBlockingQueue<T> queue) {
    this.queue = queue;
  }
}
