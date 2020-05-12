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

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.tset.StoringData;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.link.TLink;
import edu.iu.dsc.tws.api.tset.link.batch.BatchRowTLink;
import edu.iu.dsc.tws.api.tset.sets.AcceptingData;
import edu.iu.dsc.tws.api.tset.sets.TSet;
import edu.iu.dsc.tws.api.tset.table.Row;
import edu.iu.dsc.tws.api.tset.table.TableSchema;

public interface BatchRowTSet extends TBase, AcceptingData<Row>, StoringData<Row> {
  /**
   * Direct/pipe communication
   *
   * @return Keyed Direct TLink
   */
  BatchRowTLink direct();

  /**
   * Returns a Partition {@link TLink} that would partition data according based on a function
   * provided. The parallelism of the target {@link TSet} can also be specified.
   *
   * @param partitionFn       Partition function
   * @param targetParallelism Target parallelism
   * @return Partition TLink
   */
  BatchRowTLink partition(PartitionFunc<Row> partitionFn, int targetParallelism);

  /**
   * Joins with another {@link BatchTupleTSet}. Note that this TSet will be considered the left
   * TSet
   *
   * @param rightTSet     right tset
   * @param type          {@link edu.iu.dsc.tws.api.comms.CommunicationContext.JoinType}
   * @param keyComparator key comparator
   * @return Joined TLink
   */
  BatchRowTLink join(BatchRowTSet rightTSet, CommunicationContext.JoinType type,
                     Comparator<Row> keyComparator);

  /**
   * Sets the data type of the {@link TSet} output. This will be used in the packers for efficient
   * SER-DE operations in the following {@link TLink}s
   *
   * @param dataType data type as a {@link MessageType}
   * @return this {@link TSet}
   */
  BatchRowTSet withSchema(TableSchema dataType);
}
