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
package edu.iu.dsc.tws.common.util;

import java.util.NoSuchElementException;

/**
 * A double linked list with a resettable iterator
 * @param <T>
 */
public class IterativeLinkedList<T> {
  // number of elements on list
  private int numElements;
  // mark at the begining
  private Node firstMark;
  // mark at the end
  private Node lastMark;
  // the iterator
  private ILLIterator iterator;

  /**
   * List node with previous and next
    */
  private class Node {
    private T item;
    private Node next;
    private Node prev;
  }

  /**
   * Create the list
   */
  public IterativeLinkedList() {
    firstMark = new Node();
    lastMark = new Node();
    firstMark.next = lastMark;
    lastMark.prev = firstMark;
    iterator = new ILLIterator();
  }

  /**
   * Check weather empty
   * @return true if empty
   */
  public boolean isEmpty() {
    return numElements == 0;
  }

  /**
   * Size of the list
   * @return size
   */
  public int size() {
    return numElements;
  }

  /**
   * Add the element at the end of the list
    */
  public void add(T item) {
    Node last = this.lastMark.prev;
    Node x = new Node();
    x.item = item;
    x.next = this.lastMark;
    x.prev = last;
    this.lastMark.prev = x;
    last.next = x;
    numElements++;
  }

  /**
   * Create the iterator
   * @return the iterator and reset it.
   */
  public ILLIterator iterator() {
    iterator.reset();
    return iterator;
  }

  /**
   * The iterator
    */
  public class ILLIterator {
    /**
     * The current node returned by next
     */
    private Node current = firstMark.next;

    /**
     * Previous node
      */
    private Node lastAccessed = null;

    /**
     * The current index
     */
    private int index = 0;

    /**
     * Reset the iterator to begining
     */
    private void reset() {
      current = firstMark.next;
      lastAccessed = null;
      index = 0;
    }

    public boolean hasNext() {
      return index < numElements;
    }

    public boolean hasPrevious() {
      return index > 0;
    }

    /**
     * Get the next element
     * @return next element
     */
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      lastAccessed = current;
      T item = current.item;
      current = current.next;
      index++;
      return item;
    }

    /**
     * The the previous element that was accessed
     * @return previous element
     */
    public T previous() {
      if (!hasPrevious()) {
        throw new NoSuchElementException();
      }
      current = current.prev;
      index--;
      lastAccessed = current;
      return current.item;
    }

    /**
     * Remove the last element accessed
     */
    public void remove() {
      if (lastAccessed == null) {
        throw new IllegalStateException();
      }
      Node x = lastAccessed.prev;
      Node y = lastAccessed.next;
      x.next = y;
      y.prev = x;
      numElements--;
      if (current == lastAccessed) {
        current = y;
      } else {
        index--;
      }
      lastAccessed = null;
    }

    /**
     * Add the element to list
     * @param item to add
     */
    public void add(T item) {
      Node x = current.prev;
      Node y = new Node();
      Node z = current;
      y.item = item;
      x.next = y;
      y.next = z;
      z.prev = y;
      y.prev = x;
      numElements++;
      index++;
      lastAccessed = null;
    }
  }
}
