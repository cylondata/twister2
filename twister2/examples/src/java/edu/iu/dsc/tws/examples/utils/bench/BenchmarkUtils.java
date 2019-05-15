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
package edu.iu.dsc.tws.examples.utils.bench;

import java.util.logging.Logger;

import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.COLUMN_AVERAGE_TIME;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.COLUMN_TOTAL_TIME;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_ALL_RECV;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_ALL_SEND;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_MESSAGE_RECV;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_MESSAGE_SEND;

public final class BenchmarkUtils {

  private static final Logger LOG = Logger.getLogger(BenchmarkUtils.class.getName());

  private BenchmarkUtils() {
  }

  private static String generateColumnLabelWithUnit(String column, String timingFlag) {
    return String.format("%s (%s)", column, Timing.getTimingUnitForFlag(timingFlag).getLabel());
  }

  public static void markAverageTime(BenchmarkResultsRecorder recorder, boolean accept) {
    if (accept) {
      double time = Timing.averageDiff(TIMING_MESSAGE_SEND, TIMING_MESSAGE_RECV, true);
      recorder.recordColumn(
          generateColumnLabelWithUnit(COLUMN_AVERAGE_TIME, TIMING_MESSAGE_SEND),
          time
      );
      LOG.info("Average time for an iterations : " + time);
    }
  }

  public static void markTotalTime(BenchmarkResultsRecorder recorder, boolean accept) {
    if (accept) {
      double time = Timing.averageDiff(TIMING_ALL_SEND, TIMING_ALL_RECV, true);
      recorder.recordColumn(
          generateColumnLabelWithUnit(COLUMN_TOTAL_TIME, TIMING_ALL_SEND),
          time
      );
      LOG.info("Total time for all iterations : " + time);
    }
  }

  public static void markTotalAndAverageTime(BenchmarkResultsRecorder recorder, boolean accept) {
    markAverageTime(recorder, accept);
    markTotalTime(recorder, accept);
  }

}
