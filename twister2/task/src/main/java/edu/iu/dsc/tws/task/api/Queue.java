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

/**
 * Base interface for input and output queue of the task layer
 * The type of the queue can be specified
 */
public interface Queue<T> {

  /**
   * Adds the given element to the queue
   */
  boolean add(T e);

  /**
   * Removes the head of the queue
   */
  T remove();

  /**
   * Retrieves the head of the queue but does not remove it
   */
  T peek();

  /**
   * Retrieves and removes the head of the queue
   */
  T poll();

  /**
   * check if the queue is empty
   */
  boolean isEmpty();

  /**
   * get count of entries in queue
   */
  int size();
}
