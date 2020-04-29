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
package edu.iu.dsc.tws.api.tset.sets.batch;

import java.util.Comparator;
import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.tset.StoringData;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.link.batch.BatchTLink;
import edu.iu.dsc.tws.api.tset.sets.AcceptingData;
import edu.iu.dsc.tws.api.tset.table.Row;

public interface BatchRowTSet extends TBase, AcceptingData<Row>, StoringData<Row> {
  /**
   * Direct/pipe communication
   *
   * @return Keyed Direct TLink
   */
  BatchTLink<Iterator<Row>, Row> direct();

  BatchTLink<Iterator<Row>, Row> partition(PartitionFunc<Row> partitionFn, int targetParallelism);

  BatchTLink<Iterator<Row>, Row> join(BatchRowTSet rightTSet,
                                             CommunicationContext.JoinType type,
                                             Comparator<Row> keyComparator);
}
