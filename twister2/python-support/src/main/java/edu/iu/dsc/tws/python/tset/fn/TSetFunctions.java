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
package edu.iu.dsc.tws.python.tset.fn;

public final class TSetFunctions {

  private static final TSetFunctions INSTANCE = new TSetFunctions();

  private TSetFunctions() {

  }

  public static TSetFunctions getInstance() {
    return INSTANCE;
  }

  public PartitionFunctions partition() {
    return PartitionFunctions.getInstance();
  }

  public MapFunctions map() {
    return MapFunctions.getInstance();
  }

  public FlatMapFunctions flatMap() {
    return FlatMapFunctions.getInstance();
  }

  public ComputeFunctions compute() {
    return ComputeFunctions.getInstance();
  }

  public ComputeWithCollectorFunctions computeCollector() {
    return ComputeWithCollectorFunctions.getInstance();
  }

  public SinkFunctions sink() {
    return SinkFunctions.getInstance();
  }

  public ReduceFunctions reduce() {
    return ReduceFunctions.getInstance();
  }

  public ApplyFunctions apply() {
    return ApplyFunctions.getInstance();
  }
}
