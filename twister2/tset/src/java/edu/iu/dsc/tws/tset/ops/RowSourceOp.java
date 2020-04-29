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
package edu.iu.dsc.tws.tset.ops;

import java.util.Map;

import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.api.tset.table.Row;
import edu.iu.dsc.tws.tset.sets.BaseTSet;

public class RowSourceOp extends SourceOp<Row> {
  public RowSourceOp(SourceFunc<Row> src, BaseTSet originTSet,
                     Map<String, String> receivableTSets) {
    super(src, originTSet, receivableTSets);
  }

  @Override
  public void execute() {
    if (source.hasNext()) {
      Row tuple = source.next();
      multiEdgeOpAdapter.writeToEdges(tuple);
    } else {
      multiEdgeOpAdapter.writeEndToEdges();
    }
  }
}
