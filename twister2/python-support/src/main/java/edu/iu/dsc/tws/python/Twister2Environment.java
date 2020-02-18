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
package edu.iu.dsc.tws.python;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.python.numpy.NumpyHolderBuilder;
import edu.iu.dsc.tws.python.tset.PyTSetKeyedSource;
import edu.iu.dsc.tws.python.tset.PyTSetSource;
import edu.iu.dsc.tws.python.tset.fn.TSetFunctions;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.KeyedSourceTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;

public class Twister2Environment extends EntryPoint {

  private TSetEnvironment tSetEnvironment;

  Twister2Environment(TSetEnvironment tSetEnvironment) {
    this.tSetEnvironment = tSetEnvironment;
  }

  public int getWorkerId() {
    return this.tSetEnvironment.getWorkerID();
  }

  public Config getConfig() {
    return this.tSetEnvironment.getConfig();
  }

  public SourceTSet createSource(byte[] lambda, int parallelism) {
    PyTSetSource pyTSetSource = new PyTSetSource(lambda);
    return (SourceTSet) tSetEnvironment.createSource(pyTSetSource, parallelism);
  }

  public SourceTSet parallelize(List data, int parallelism) {
    return (SourceTSet) tSetEnvironment.parallelize(data, parallelism);
  }

  public KeyedSourceTSet parallelize(Map map, int parallelism) {
    return (KeyedSourceTSet) tSetEnvironment.parallelize(map, parallelism);
  }

  public KeyedSourceTSet parallelize(Map map, Comparator comparator, int parallelism) {
    return (KeyedSourceTSet) tSetEnvironment.parallelize(map, parallelism, comparator);
  }

  public KeyedSourceTSet createKeyedSource(byte[] lambda, int parallelism) {
    PyTSetKeyedSource pyTSetSource = new PyTSetKeyedSource(lambda);
    return (KeyedSourceTSet) tSetEnvironment.createKeyedSource(pyTSetSource, parallelism);
  }

  public TSetFunctions functions() {
    return TSetFunctions.getInstance();
  }

  public NumpyHolderBuilder getNumpyBuilder() {
    return NumpyHolderBuilder.getInstance();
  }
}
