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

package edu.iu.dsc.tws.tset.env;

import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.fn.ArrowBasedSourceFunc;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.tset.fn.impl.CSVBasedSourceFunction;
import edu.iu.dsc.tws.tset.fn.impl.TextBasedSourceFunction;
import edu.iu.dsc.tws.tset.sets.streaming.SKeyedSourceTSet;
import edu.iu.dsc.tws.tset.sets.streaming.SSourceTSet;

/**
 * Implementation of {@link TSetEnvironment} for streaming {@link OperationMode}.
 * <p>
 * There is only a single execution mode that would run the entire TSet graph.
 */
public class StreamingTSetEnvironment extends TSetEnvironment {

  public StreamingTSetEnvironment(WorkerEnvironment wEnv) {
    super(wEnv);
  }

  public StreamingTSetEnvironment() {
    super();
  }

  @Override
  public OperationMode getOperationMode() {
    return OperationMode.STREAMING;
  }

  @Override
  public <T> SSourceTSet<T> createSource(SourceFunc<T> source, int parallelism) {
    SSourceTSet<T> sourceT = new SSourceTSet<>(this, source, parallelism);
    getGraph().addSourceTSet(sourceT);

    return sourceT;
  }

  @Override
  public <T> SSourceTSet<T> createSource(String name, SourceFunc<T> source, int parallelism) {
    SSourceTSet<T> sourceT = new SSourceTSet<>(this, name, source, parallelism);
    getGraph().addSourceTSet(sourceT);

    return sourceT;
  }

  public SSourceTSet<String[]> createCSVSource(String filePath, int datasize, int parallelism,
                                            String type) {
    return createSource(new CSVBasedSourceFunction(filePath, datasize, parallelism, type),
        parallelism);
  }

  public SSourceTSet<String> createTextSource(String filePath, int dataSize, int parallelism,
                                           String type) {
    return createSource(new TextBasedSourceFunction(filePath, dataSize, parallelism, type),
        parallelism);
  }

  public SSourceTSet<String> createArrowSource(String filePath, int parallelism,
                                                Schema arrowschema) {
    try {
      return createSource(
          new ArrowBasedSourceFunc(filePath, parallelism, arrowschema), parallelism);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public <K, V> SKeyedSourceTSet<K, V> createKeyedSource(SourceFunc<Tuple<K, V>> source,
                                                         int parallelism) {
    SKeyedSourceTSet<K, V> sourceT = new SKeyedSourceTSet<>(this, source, parallelism);
    getGraph().addSourceTSet(sourceT);
    return sourceT;
  }

  @Override
  public <K, V> SKeyedSourceTSet<K, V> createKeyedSource(String name,
                                                         SourceFunc<Tuple<K, V>> source,
                                                         int parallelism) {
    SKeyedSourceTSet<K, V> sourceT = new SKeyedSourceTSet<>(this, name, source, parallelism);
    getGraph().addSourceTSet(sourceT);
    return sourceT;
  }

  /**
   * Runs the entire TSet graph
   */
  public void run() {
    BuildContext buildCtx = getTSetGraph().build();
    executeBuildContext(buildCtx);
  }

}
