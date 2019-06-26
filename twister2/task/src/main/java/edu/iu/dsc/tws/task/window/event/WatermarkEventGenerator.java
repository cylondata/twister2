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
package edu.iu.dsc.tws.task.window.event;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import edu.iu.dsc.tws.task.window.api.GlobalStreamId;
import edu.iu.dsc.tws.task.window.exceptions.FailedException;
import edu.iu.dsc.tws.task.window.manage.WindowManager;

public class WatermarkEventGenerator<T> implements Runnable {

  private static final Logger LOG = Logger.getLogger(WatermarkEventGenerator.class.getName());
  private final WindowManager<T> windowManager;
  private final long eventLagTime;
  private final ScheduledExecutorService executorService;
  private final long interval;
  private ScheduledFuture<?> executorFuture;
  private volatile long lastWatermarkTime;
  private volatile long currentProcessedMessageTime = 0;
  private Set<GlobalStreamId> inputStreams;
  private final Map<GlobalStreamId, Long> streamTimeStampMap;


  public WatermarkEventGenerator(WindowManager<T> winManager, long eventLagTime, long interval,
                                 Set<GlobalStreamId> inputStreams) {
    this.windowManager = winManager;
    this.eventLagTime = eventLagTime;
    this.interval = interval;
    this.inputStreams = inputStreams;
    this.streamTimeStampMap = new ConcurrentHashMap<>();
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("tws-watermark-event-generator-%d")
        .setDaemon(true)
        .build();
    executorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
  }

  /**
   * replaced with computeWaterMarkTimeStamp   *
   * @deprecated this method is depreacted due to GlobalStreamID logic
   *
   */
  @Deprecated
  private long computeWaterMark() {
    long t = 0;
    //t = Long.MAX_VALUE;
    return t - eventLagTime;
  }

  private long computeWaterMarkTimeStamp() {
    long timestamp = 0;
    if (streamTimeStampMap.size() >= this.inputStreams.size()) {
      timestamp = Long.MAX_VALUE;
      for (Map.Entry<GlobalStreamId, Long> entry : streamTimeStampMap.entrySet()) {
        timestamp = Math.min(timestamp, entry.getValue());
      }
    }
    return timestamp - eventLagTime;
  }

  @Override
  public void run() {
    try {
      long watermarkTime = computeWaterMarkTimeStamp();
      if (watermarkTime > lastWatermarkTime) {
        this.windowManager.add(new WatermarkEvent<>(watermarkTime));
        lastWatermarkTime = watermarkTime;
        LOG.fine(String.format("WaterMark event added"));
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

  public boolean track(GlobalStreamId globalStreamId, long timestamp) {
    Long currentValue = streamTimeStampMap.get(globalStreamId);
    if (currentValue == null || timestamp > currentValue) {
      streamTimeStampMap.put(globalStreamId, timestamp);
    }
    checkFailures();
    return timestamp >= lastWatermarkTime;
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
    //TODO :: configurable timing
    this.executorFuture = executorService.scheduleAtFixedRate(this, interval, interval,
        TimeUnit.MILLISECONDS);
  }

  public void shutdown() {
    LOG.info("Shutting Down WatermarkGenerator");
    executorService.shutdown();
    // TODO :: configurable watermark generator timing
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
