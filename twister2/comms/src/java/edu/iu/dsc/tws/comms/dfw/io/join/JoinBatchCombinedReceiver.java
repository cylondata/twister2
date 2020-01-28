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
package edu.iu.dsc.tws.comms.dfw.io.join;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.comms.shuffle.ResettableIterator;
import edu.iu.dsc.tws.comms.shuffle.RestorableIterator;
import edu.iu.dsc.tws.comms.utils.HashJoinUtils;
import edu.iu.dsc.tws.comms.utils.JoinRelation;
import edu.iu.dsc.tws.comms.utils.KeyComparatorWrapper;
import edu.iu.dsc.tws.comms.utils.SortJoinUtils;

/**
 * This class performs the join operation when all the relations are ready
 * <p>
 * For now, twister2 support only joining two relations. This class is written to
 * support more than two relations in future.
 */
public class JoinBatchCombinedReceiver {

  private static final Logger LOG = Logger.getLogger(JoinBatchCombinedReceiver.class.getName());

  private Map<Integer, boolean[]> syncCounts = new HashMap<>();
  private Map<Integer, Object[]> joinRelations = new HashMap<>();
  private BulkReceiver rcvr;
  private final CommunicationContext.JoinAlgorithm algorithm;
  private final CommunicationContext.JoinType joinType;
  private KeyComparatorWrapper keyComparator;
  private MessageType keyType;

  public JoinBatchCombinedReceiver(BulkReceiver recvr,
                                   CommunicationContext.JoinAlgorithm algorithm,
                                   CommunicationContext.JoinType joinType,
                                   KeyComparatorWrapper keyComparator, MessageType keyType) {
    this.rcvr = recvr;
    this.algorithm = algorithm;
    this.joinType = joinType;
    this.keyComparator = keyComparator;
    this.keyType = keyType;
  }

  public void init(Config cfg, Set<Integer> targets) {
    for (Integer target : targets) {
      boolean[] syncs = new boolean[JoinRelation.values().length];
      Arrays.fill(syncs, false);
      syncCounts.put(target, syncs);

      Object[] values = new Object[JoinRelation.values().length];
      Arrays.fill(values, null);
      joinRelations.put(target, values);
    }
  }

  private Iterator doJoin(Object left, Object right) {
    if (algorithm.equals(CommunicationContext.JoinAlgorithm.SORT)) {
      if (left instanceof RestorableIterator) {
        return SortJoinUtils.join(
            (RestorableIterator) left,
            (RestorableIterator) right,
            keyComparator, joinType);
      } else if (left instanceof List) {
        return SortJoinUtils.join(
            (List<Tuple>) left,
            (List<Tuple>) right,
            keyComparator, joinType).iterator();
      } else {
        throw new Twister2RuntimeException("Unsupported data formats received from sources : "
            + left.getClass());
      }
    } else {
      if (left instanceof ResettableIterator) {
        return HashJoinUtils.join(
            (ResettableIterator) left,
            (ResettableIterator) right,
            joinType,
            keyType
        );
      } else if (left instanceof List) {
        return HashJoinUtils.join(
            (List<Tuple>) left,
            (List<Tuple>) right,
            joinType,
            keyType
        ).iterator();
      } else {
        throw new Twister2RuntimeException("Unsupported data formats received from sources");
      }
    }
  }

  public boolean receive(int target, Object object, JoinRelation joinRelation) {
    Object[] values = joinRelations.get(target);
    values[joinRelation.ordinal()] = object;
    long count = Arrays.stream(values).filter(Objects::nonNull).count();

    if (count == JoinRelation.values().length) {
      // ready to do join
      long t1 = System.currentTimeMillis();
      this.rcvr.receive(target, doJoin(values[JoinRelation.LEFT.ordinal()],
          values[JoinRelation.RIGHT.ordinal()]));
      LOG.info("Join time : " + (System.currentTimeMillis() - t1));

      Arrays.fill(values, null);
    }
    return true;
  }

  private boolean isAllSynced(int target, JoinRelation newSync) {
    boolean[] syncState = syncCounts.get(target);
    syncState[newSync.ordinal()] = true;
    for (JoinRelation joinRelation : JoinRelation.values()) {
      if (!syncState[joinRelation.ordinal()]) {
        return false;
      }
    }

    // if all synced, reset the status
    Arrays.fill(syncState, false);
    return true;
  }

  public boolean sync(int target, byte[] message, JoinRelation joinRelation) {
    if (isAllSynced(target, joinRelation)) {
      rcvr.sync(target, message);
    }
    return true;
  }
}
