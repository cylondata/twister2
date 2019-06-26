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
package edu.iu.dsc.tws.task.window.util;

import java.util.logging.Logger;

import edu.iu.dsc.tws.task.window.api.IWindow;
import edu.iu.dsc.tws.task.window.config.SlidingCountWindow;
import edu.iu.dsc.tws.task.window.config.SlidingDurationWindow;
import edu.iu.dsc.tws.task.window.config.TumblingCountWindow;
import edu.iu.dsc.tws.task.window.config.TumblingDurationWindow;
import edu.iu.dsc.tws.task.window.config.WindowConfig;
import edu.iu.dsc.tws.task.window.constant.WindowBuffer;
import edu.iu.dsc.tws.task.window.exceptions.InvalidWindow;

public final class WindowUtils {

  private static final Logger LOG = Logger.getLogger(WindowUtils.class.getName());

  private WindowUtils() {

  }

  public static IWindow getWindow(WindowConfig.Count windowCount,
                                  WindowConfig.Count slidingCount,
                                  WindowConfig.Duration windowDuration,
                                  WindowConfig.Duration slidingDuration)
      throws InvalidWindow {

    IWindow iWindow = null;

    if (windowCount != null && slidingCount != null) {
      iWindow = checkType(windowCount.value, slidingCount.value, WindowBuffer.Count);
    }

    if (windowDuration != null && slidingDuration != null) {
      LOG.info(String.format("Duration GetWindow"));
      iWindow = checkType(windowDuration.value, slidingDuration.value, WindowBuffer.Duration);
    }

    return iWindow;
  }

  private static IWindow checkType(long windowLength, long slidingLength, WindowBuffer buffer)
      throws InvalidWindow {
    IWindow window = null;
    if (windowLength > 0 && slidingLength > 0 && buffer != null) {
      if (windowLength == slidingLength) {
        // tumbling window
        if (buffer == WindowBuffer.Count) {
          // tumbling count window
          LOG.info(String.format("Selecting Tumbling Count Window"));
          window = new TumblingCountWindow(windowLength);
        }
        if (buffer == WindowBuffer.Duration) {
          // tumbling duration window
          LOG.info(String.format("Selecting Tumbling Duration Window"));
          window = new TumblingDurationWindow(windowLength);
        }

      } else if (windowLength > slidingLength) {
        // sliding window
        if (buffer == WindowBuffer.Count) {
          LOG.info(String.format("Selecting Sliding Count Window"));
          window = new SlidingCountWindow(windowLength, slidingLength);
        }

        if (buffer == WindowBuffer.Duration) {
          LOG.info(String.format("Selecting Sliding Duration Window"));
          window = new SlidingDurationWindow(windowLength, slidingLength);
        }
        //throw new RuntimeException("Not Implemented");
      } else {
        throw new InvalidWindow("Invalid window, window size > sliding length");
      }
    } else {
      throw new InvalidWindow("Window Length and Sliding Length must be greater than zero");
    }
    return window;
  }

}
