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
package edu.iu.dsc.tws.task.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.executor.IExecutor;
import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.faulttolerance.Fault;

public class ExecutorList {
  private static final Logger LOG = Logger.getLogger(ExecutorList.class.getName());
  /**
   * The current executions that are happening
   */
  private List<IExecutor> currentExecutors = Collections.synchronizedList(new ArrayList<>());

  /**
   * Lock to guard access
   */
  private Lock lock = new ReentrantLock();

  void onFault(Fault fault) {
    lock.lock();
    try {
      for (IExecutor i : currentExecutors) {
        i.onFault(fault);
      }
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, "An error occurred while setting fault", e);
      throw new Twister2RuntimeException("An error occurred while setting fault", e);
    } finally {
      lock.unlock();
    }
  }

  void add(IExecutor executor) {
    lock.lock();
    try {
      currentExecutors.add(executor);
    } finally {
      lock.unlock();
    }
  }

  synchronized void remove(IExecutor remove) {
    lock.lock();
    try {
      currentExecutors.remove(remove);
    } finally {
      lock.unlock();
    }
  }
}
