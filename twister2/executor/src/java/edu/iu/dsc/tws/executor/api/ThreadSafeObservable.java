//  Copyright 2017 Twitter. All rights reserved.
//
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
package edu.iu.dsc.tws.executor.api;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by vibhatha on 8/24/17.
 */
public class ThreadSafeObservable {

  // this is the object we will be synchronizing on ("the monitor")
  private final Object MONITOR = new Object();

  private Set<IObserver> mObservers;

  /**
   * This method adds a new Observer - it will be notified when Observable changes
   */
  public void registerObserver(IObserver observer) {
    if (observer == null) return;

    synchronized(MONITOR) {
      if (mObservers == null) {
        mObservers = new HashSet<>(1);
      }
      if (mObservers.add(observer) && mObservers.size() == 1) {
        performInit(); // some initialization when first observer added
      }
    }
  }

  private void performInit() {
    System.out.println("Initialization ");
  }

  private void performCleanup(){
    System.out.println("Clean Up ");
  }

  /**
   * This method removes an Observer - it will no longer be notified when Observable changes
   */
  public void unregisterObserver(IObserver observer) {
    if (observer == null) return;

    synchronized(MONITOR) {
      if (mObservers != null && mObservers.remove(observer) && mObservers.isEmpty()) {
        performCleanup(); // some cleanup when last observer removed
      }
    }
  }

  /**
   * This method notifies currently registered observers about Observable's change
   */
  private void notifyObservers() {
    Set<IObserver> observersCopy;

    synchronized(MONITOR) {
      if (mObservers == null) return;
      observersCopy = new HashSet<>(mObservers);
    }

    for (IObserver observer : observersCopy) {
      observer.onObservableChanged();
    }
  }
}