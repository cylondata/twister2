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

public class IterativeLinkedList<T> {
  // number of elements on list
  private int n;
  // sentinel before first item
  private Node pre;
  // sentinel after last item
  private Node post;
  // the iterator
  private ILLIterator iterator;

  public IterativeLinkedList() {
    pre = new Node();
    post = new Node();
    pre.next = post;
    post.prev = pre;
    iterator = new ILLIterator();
  }

  // linked list node helper data type
  private class Node {
    private T item;
    private Node next;
    private Node prev;
  }

  public boolean isEmpty() {
    return n == 0;
  }

  public int size() {
    return n;
  }

  // add the item to the list
  public void add(T item) {
    Node last = post.prev;
    Node x = new Node();
    x.item = item;
    x.next = post;
    x.prev = last;
    post.prev = x;
    last.next = x;
    n++;
  }

  public ILLIterator iterator() {
    iterator.reset();
    return iterator;
  }

  // assumes no calls to IterativeLinkedList.add() during iteration
  public class ILLIterator {
    // the node that is returned by next()
    private Node current = pre.next;
    // the last node to be returned by prev() or next()
    private Node lastAccessed = null;

    // reset to null upon intervening remove() or add()
    private int index = 0;

    private void reset() {
      current = pre.next;
      lastAccessed = null;
      index = 0;
    }

    public boolean hasNext() {
      return index < n;
    }

    public boolean hasPrevious() {
      return index > 0;
    }

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

    public T previous() {
      if (!hasPrevious()) {
        throw new NoSuchElementException();
      }
      current = current.prev;
      index--;
      lastAccessed = current;
      return current.item;
    }

    // replace the item of the element that was last accessed by next() or previous()
    // condition: no calls to remove() or add() after last call to next() or previous()
    public void set(T item) {
      if (lastAccessed == null) {
        throw new IllegalStateException();
      }
      lastAccessed.item = item;
    }

    // remove the element that was last accessed by next() or previous()
    // condition: no calls to remove() or add() after last call to next() or previous()
    public void remove() {
      if (lastAccessed == null) {
        throw new IllegalStateException();
      }
      Node x = lastAccessed.prev;
      Node y = lastAccessed.next;
      x.next = y;
      y.prev = x;
      n--;
      if (current == lastAccessed) {
        current = y;
      } else {
        index--;
      }
      lastAccessed = null;
    }

    // add element to list
    public void add(T item) {
      Node x = current.prev;
      Node y = new Node();
      Node z = current;
      y.item = item;
      x.next = y;
      y.next = z;
      z.prev = y;
      y.prev = x;
      n++;
      index++;
      lastAccessed = null;
    }
  }
}
