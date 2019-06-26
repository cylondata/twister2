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
package edu.iu.dsc.tws.task.window.policy.trigger.duration;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import edu.iu.dsc.tws.task.window.api.DefaultEvictionContext;
import edu.iu.dsc.tws.task.window.api.Event;
import edu.iu.dsc.tws.task.window.api.IEvictionPolicy;
import edu.iu.dsc.tws.task.window.manage.IManager;
import edu.iu.dsc.tws.task.window.policy.trigger.IWindowingPolicy;

public class DurationWindowPolicy<T> implements IWindowingPolicy<T> {

  private static final Logger LOG = Logger.getLogger(DurationWindowPolicy.class.getName());

  private static final long TIMEOUT = 2;

  private static final long TRIGGER_CALL_TIME = 1;

  private long duration;
  private final ScheduledExecutorService executor;
  private ScheduledFuture<?> executorFuture;
  private final IManager manager;
  private final IEvictionPolicy<T> evictionPolicy;

  public DurationWindowPolicy(long millis, IManager<T> mgr, IEvictionPolicy<T> policy) {
    this.duration = millis;
    this.manager = mgr;
    this.evictionPolicy = policy;
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("duration-trigger-policy-%d")
        .setDaemon(true)
        .build();
    this.executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
  }

  @Override
  public boolean validate() {
    return this.duration > 0;
  }

  @Override
  public String whyInvalid() {
    return null;
  }

  @Override
  public void track(Event<T> event) {
    checkFailures();
  }

  @Override
  public void reset() {
    checkFailures();
  }

  private void checkFailures() {
    if (executorFuture != null && executorFuture.isDone()) {
      try {
        executorFuture.get();
      } catch (InterruptedException ex) {
        LOG.severe(String.format("Exception %s", ex.getMessage()));
        //throw new FailureException(ex.getMessage());
      } catch (ExecutionException ex) {
        LOG.severe(String.format("Exception %s", ex.getMessage()));
        //throw new FailureException(ex.getMessage());
      }
    }
  }

  @Override
  public void start() {
    this.executorFuture = executor.scheduleAtFixedRate(newTriggerTask(), this.duration,
        this.duration, TimeUnit.MILLISECONDS);
  }

  @Override
  public void shutdown() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(TIMEOUT, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException ie) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private Runnable newTriggerTask() {
    return new Runnable() {
      @Override
      public void run() {
        // do not process current timestamp since tuples might arrive while the trigger is executing
        long now = System.currentTimeMillis() - TRIGGER_CALL_TIME;
        try {
          /*
           * set the current timestamp as the reference time for the eviction policy
           * to evict the events
           */
          if (evictionPolicy != null) {
            evictionPolicy.setContext(new DefaultEvictionContext(now, null, null, duration));
          }
          manager.onEvent();
        } catch (Throwable th) {
          LOG.severe(String.format("manager.onEvent failed %s", th.getMessage()));
          /*
           * propagate it so that task gets canceled and the exception
           * can be retrieved from executorFuture.get()
           */
          throw th;
        }
      }
    };
  }

  @Override
  public String toString() {
    return "DurationWindowPolicy{"
        + "duration=" + duration
        + '}';
  }
}
