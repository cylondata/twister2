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
package edu.iu.dsc.tws.tset.sets.batch;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.link.batch.BatchTLink;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchRowTSet;
import edu.iu.dsc.tws.api.tset.table.Row;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.tset.TBaseGraph;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.links.batch.RowDirectLink;
import edu.iu.dsc.tws.tset.sets.BaseTSetWithSchema;

public abstract class BatchRowTSetImpl extends BaseTSetWithSchema<Row> implements BatchRowTSet {

  BatchRowTSetImpl(BatchTSetEnvironment tSetEnv, String name, int parallelism, Schema inputSchema) {
    super(tSetEnv, name, parallelism, inputSchema);
  }

  @Override
  public BatchTLink<Iterator<Row>, Row> partition(PartitionFunc<Row> partitionFn,
                                                  int targetParallelism) {
    return null;
  }

  @Override
  public BatchTLink<Iterator<Row>, Row> join(BatchRowTSet rightTSet,
                                             CommunicationContext.JoinType type,
                                             Comparator<Row> keyComparator) {
    return null;
  }

  @Override
  public BatchTSetEnvironment getTSetEnv() {
    return (BatchTSetEnvironment) super.getTSetEnv();
  }

  @Override
  public BatchTLink<Iterator<Row>, Row> direct() {
    RowDirectLink direct = new RowDirectLink(getTSetEnv(),
        getParallelism(), getOutputSchema());
    addChildToGraph(direct);
    return direct;
  }

  @Override
  public BatchRowTSetImpl setName(String n) {
    rename(n);
    return this;
  }

  @Override
  public StorableTBase<Row> cache() {
    return direct().cache();
  }

  @Override
  public StorableTBase<Row> lazyCache() {
    return direct().lazyCache();
  }

  @Override
  public StorableTBase<Row> persist() {
    return direct().persist();
  }

  @Override
  public StorableTBase<Row> lazyPersist() {
    return direct().lazyPersist();
  }

  @Override
  public TBase addInput(String key, StorableTBase<?> input) {
    return direct().lazyPersist();
  }

  @Override
  public void build(GraphBuilder graphBuilder, Collection<? extends TBase> buildSequence) {

  }

  @Override
  public TBaseGraph getTBaseGraph() {
    return null;
  }

  @Override
  public String generateID(String prefix) {
    return null;
  }
}
