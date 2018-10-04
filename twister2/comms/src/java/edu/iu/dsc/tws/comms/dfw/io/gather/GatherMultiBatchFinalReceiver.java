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
package edu.iu.dsc.tws.comms.dfw.io.gather;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MultiMessageReceiver;
import edu.iu.dsc.tws.comms.dfw.io.KeyedReceiver;
import edu.iu.dsc.tws.comms.dfw.io.gather.keyed.DKGatherBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.gather.keyed.KGatherBatchFinalReceiver;

public class GatherMultiBatchFinalReceiver implements MultiMessageReceiver {

  /**
   * The final receiver specified by the user
   */
  private BulkReceiver bulkReceiver;

  /**
   * Specifies if the gather operation uses disk for saving. if shuffle is true it indicates that
   * the results will be saved to the disk and retrieved when needed.
   */
  private boolean shuffle;

  /**
   * Map that keeps final receivers for each target
   */
  private Map<Integer, KeyedReceiver> receiverMap = new HashMap<>();

  /**
   * Shuffler directory
   */
  private String shuffleDirectory;

  /**
   * weather we need to sort the records according to key
   */
  private boolean sorted;

  /**
   * Comparator for sorting records
   */
  private Comparator<Object> comparator;

  public GatherMultiBatchFinalReceiver(BulkReceiver receiver, boolean shuffle, boolean sorted,
                                       String shuffleDir, Comparator<Object> comparator) {
    this.bulkReceiver = receiver;
    this.shuffle = shuffle;
    this.sorted = sorted;
    this.shuffleDirectory = shuffleDir;
    this.comparator = comparator;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op,
                   Map<Integer, Map<Integer, List<Integer>>> expectedIds) {
    for (Map.Entry<Integer, Map<Integer, List<Integer>>> e : expectedIds.entrySet()) {
      if (!shuffle) {
        KGatherBatchFinalReceiver finalReceiver = new KGatherBatchFinalReceiver(
            bulkReceiver, 10);
        receiverMap.put(e.getKey(), finalReceiver);
        finalReceiver.init(cfg, op, e.getValue());
      } else {
        DKGatherBatchFinalReceiver finalReceiver = new DKGatherBatchFinalReceiver(
            bulkReceiver, sorted, 10, shuffleDirectory, comparator);
        receiverMap.put(e.getKey(), finalReceiver);
        finalReceiver.init(cfg, op, e.getValue());
      }


    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    KeyedReceiver finalReceiver = receiverMap.get(path);
    return finalReceiver.onMessage(source, path, target, flags, object);
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;

    for (Map.Entry<Integer, KeyedReceiver> e : receiverMap.entrySet()) {
      needsFurtherProgress = needsFurtherProgress | e.getValue().progress();
    }
    return needsFurtherProgress;
  }
}
