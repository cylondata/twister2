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
package edu.iu.dsc.tws.task.api.window.event;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import edu.iu.dsc.tws.task.api.window.exceptions.FailedException;
import edu.iu.dsc.tws.task.api.window.manage.WindowManager;

public class WatermarkEventGenerator<T> implements Runnable {

  private static final Logger LOG = Logger.getLogger(WatermarkEventGenerator.class.getName());
  private final WindowManager<T> windowManager;
  private final long eventLagTime;
  private final ScheduledExecutorService executorService;
  private final long interval;
  private ScheduledFuture<?> executorFuture;
  private volatile long lastWatermarkTime;
  private volatile long currentProcessedMessageTime = 0;

  public WatermarkEventGenerator(WindowManager<T> winManager, long eventLagTime, long interval) {
    this.windowManager = winManager;
    this.eventLagTime = eventLagTime;
    this.interval = interval;

    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("tws-watermark-event-generator-%d")
        .setDaemon(true)
        .build();
    executorService = Executors.newSingleThreadScheduledExecutor(threadFactory);

  }

  private long computeWaterMark() {
    long t = 0;
    t = Long.MAX_VALUE;
    return t - eventLagTime;
  }

  @Override
  public void run() {
    try {
      long watermarkTime = computeWaterMark();
      if (watermarkTime > lastWatermarkTime) {
        this.windowManager.add(new WatermarkEvent<>(watermarkTime));
        lastWatermarkTime = watermarkTime;
      }
    } catch (Throwable throwable) {
      LOG.severe(String.format("Failure occurred in the watermarked event %s ",
          throwable.getMessage()));
    }
  }

  public boolean track(long time) {
    Long currentValue = currentProcessedMessageTime;
    if (currentValue == 0 || time > currentValue) {
      currentProcessedMessageTime = time;
    }
    checkFailures();
    return time >= lastWatermarkTime;
  }


  private void checkFailures() {
    if (executorFuture != null && executorFuture.isDone()) {
      try {
        executorFuture.get();
      } catch (InterruptedException ex) {
        LOG.severe(String.format("Exception Occurred : %s", ex.getMessage()));
        throw new FailedException(ex);
      } catch (ExecutionException ex) {
        LOG.severe(String.format("Exception Occurred : %s", ex.getMessage()));
        throw new FailedException(ex);
      }
    }
  }

  public void start() {
    this.executorFuture = executorService.scheduleAtFixedRate(this, interval, interval,
        TimeUnit.MILLISECONDS);
  }

  public void shutdown() {
    LOG.info("Shutting Down WatermarkGenerator");
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(2, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException ie) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
