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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Timing {

  private static Map<String, List<Long>> timestamps = new HashMap<>();

  private static int onWkr = -1;
  private static int thisWkr = -2;

  private Timing() {
  }

  public static void activate(int onWorker, int thisWorker) {
    Timing.onWkr = onWorker;
    Timing.thisWkr = thisWorker;
  }

  public static void defineFlag(String flag, int size) {
    timestamps.put(flag, new ArrayList<>(size));
  }

  public static synchronized void mark(String flag, TimingUnit unit) {
    timestamps.computeIfAbsent(flag, s -> new ArrayList<>())
        .add(unit.getTime());
  }

  public static synchronized void markMili(String flag) {
    mark(flag, TimingUnit.MILLI_SECONDS);
  }

  public static synchronized void markNano(String flag) {
    mark(flag, TimingUnit.NANO_SECONDS);
  }

  private static void verifyTwoFlags(String flagA, String flagB) {
    if (timestamps.get(flagA).size() != timestamps.get(flagB).size()) {
      throw new RuntimeException("Collected data for two flags mismatches");
    }
  }

  public static long averageDiff(String flagA, String flagB) {
    if (onWkr != thisWkr) {
      return -1;
    }
    verifyTwoFlags(flagA, flagB);

    List<Long> flagALongs = timestamps.get(flagA);
    List<Long> flagBLongs = timestamps.get(flagB);

    long totalDiffs = 0;
    for (int i = 0; i < flagALongs.size(); i++) {
      totalDiffs = flagBLongs.get(i) - flagALongs.get(i);
    }

    return totalDiffs / flagALongs.size();
  }

  public static List<Long> diffs(String flagA, String flagB) {
    if (onWkr != thisWkr) {
      return Collections.emptyList();
    }
    verifyTwoFlags(flagA, flagB);

    List<Long> flagALongs = timestamps.get(flagA);
    List<Long> flagBLongs = timestamps.get(flagB);

    List<Long> diffs = new ArrayList<>(flagALongs.size());

    for (int i = 0; i < flagALongs.size(); i++) {
      diffs.add(flagBLongs.get(i) - flagALongs.get(i));
    }

    return diffs;
  }

}
