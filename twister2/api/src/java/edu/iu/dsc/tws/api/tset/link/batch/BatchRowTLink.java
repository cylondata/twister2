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
package edu.iu.dsc.tws.api.tset.link.batch;

import java.util.Iterator;

import edu.iu.dsc.tws.api.tset.StoringData;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchRowTSet;
import edu.iu.dsc.tws.api.tset.table.Row;

/**
 * Table based communication links.
 */
public interface BatchRowTLink extends StoringData<Row> {
  BatchRowTSet compute(ComputeCollectorFunc<Row, Iterator<Row>> computeFunction);

  BatchRowTSet map(MapFunc<Row, Row> mapFn);

  BatchRowTSet flatmap(FlatMapFunc<Row, Row> mapFn);

  TBase sink(SinkFunc<Row> sinkFunction);

  void forEach(ApplyFunc<Row> applyFunction);

  BatchRowTSet lazyForEach(ApplyFunc<Row> applyFunction);
}
