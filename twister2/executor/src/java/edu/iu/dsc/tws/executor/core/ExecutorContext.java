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
package edu.iu.dsc.tws.executor.core;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;

public class ExecutorContext extends Context {
  public static final String THREADS_PER_WORKER = "twister2.exector.worker.threads";
  public static final String INSTANCE_QUEUE_LOW_WATERMARK =
      "twister2.exector.instance.queue.low.watermark";
  public static final String INSTANCE_QUEUE_HIGH_WATERMARK =
      "twister2.exector.instance.queue.high.watermark";

  public static int threadsPerContainer(Config cfg) {
    return cfg.getIntegerValue(THREADS_PER_WORKER, 1);
  }

  public static int instanceQueueLowWaterMark(Config cfg) {
    return cfg.getIntegerValue(INSTANCE_QUEUE_LOW_WATERMARK, 64);
  }

  public static int instanceQueueHighWaterMark(Config cfg) {
    return cfg.getIntegerValue(INSTANCE_QUEUE_LOW_WATERMARK, 128);
  }
}
