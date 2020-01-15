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
package edu.iu.dsc.tws.tset.sets.streaming;

import java.util.Collections;
import java.util.Iterator;

import edu.iu.dsc.tws.api.compute.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.TFunction;
import edu.iu.dsc.tws.task.window.util.WindowParameter;
import edu.iu.dsc.tws.tset.env.StreamingTSetEnvironment;
import edu.iu.dsc.tws.tset.fn.AggregateFunc;
import edu.iu.dsc.tws.tset.fn.WindowComputeFunc;
import edu.iu.dsc.tws.tset.ops.WindowComputeOp;

/**
 * WindowComputeTSet is the TSet abstraction designed for windowing. This class contains windowing
 * functions.
 * 1. Process Function (calls the compute function and process the TSet elements on user the
 * defined function)
 * 2. Aggregate Function (calls the compute and do the TSet element aggregation on user-defined
 * logic.
 *
 * @param <O> Output type of TSet
 * @param <I> Input Type of TSet
 */
public class WindowComputeTSet<O, I> extends StreamingTSetImpl<O> {
  private TFunction<O, I> computeFunc;

  private WindowParameter windowParameter;

  public WindowComputeTSet(StreamingTSetEnvironment tSetEnv, ComputeFunc<O, I> computeFunction,
                           int parallelism, WindowParameter winParam) {
    this(tSetEnv, "wcompute", computeFunction, parallelism, winParam);
  }

  public WindowComputeTSet(StreamingTSetEnvironment tSetEnv,
                           int parallelism, WindowParameter winParam) {
    this(tSetEnv, "wcompute", parallelism, winParam);
  }

  public WindowComputeTSet(StreamingTSetEnvironment tSetEnv, ComputeCollectorFunc<O, I> compOp,
                           int parallelism, WindowParameter winParam) {
    this(tSetEnv, "wcomputec", compOp, parallelism, winParam);
  }

  public WindowComputeTSet(StreamingTSetEnvironment tSetEnv, String name,
                           ComputeFunc<O, I> computeFunction, int parallelism,
                           WindowParameter winParam) {
    super(tSetEnv, name, parallelism);
    this.computeFunc = computeFunction;
    this.windowParameter = winParam;
  }

  public WindowComputeTSet(StreamingTSetEnvironment tSetEnv, String name, int parallelism,
                           WindowParameter winParam) {
    super(tSetEnv, name, parallelism);
    this.windowParameter = winParam;
  }

  public WindowComputeTSet(StreamingTSetEnvironment tSetEnv, String name,
                           ComputeCollectorFunc<O, I> compOp, int parallelism,
                           WindowParameter winParam) {
    super(tSetEnv, name, parallelism);
    this.computeFunc = compOp;
    this.windowParameter = winParam;
  }

  @Override
  public WindowComputeTSet<O, I> setName(String name) {
    rename(name);
    return this;
  }


  @Override
  public ICompute<I> getINode() {
    // todo: fix empty map (will have to handle inputs to window functions)
    if (computeFunc instanceof ComputeFunc) {
      return new WindowComputeOp<>((ComputeFunc<O, Iterator<I>>) computeFunc, this,
          Collections.emptyMap(), windowParameter);
    } else {
      throw new RuntimeException("Unknown function type for window compute: " + computeFunc);
    }
  }

  public WindowComputeTSet<O, I> process(WindowComputeFunc<O, I> processFunction) {
    if (this.computeFunc == null) {
      this.computeFunc = processFunction;
      return this;
    } else {
      throw new RuntimeException("process/aggregate can only be called once on a WindowComputeTSet "
          + getName());
    }
  }

  /**
   * This method reduces the values inside a window
   *
   * @param aggregateFunction reduce function definition
   * @return reduced value of type O
   */
  public WindowComputeTSet<O, I> aggregate(AggregateFunc<O> aggregateFunction) {

    this.process(new WindowComputeFunc<O, I>() {
      @Override
      public O compute(I input) {
        O initial = null;
        if (input instanceof Iterator) {
          Iterator<O> itr = (Iterator<O>) input;
          while (itr.hasNext()) {
            if (initial == null) {
              initial = itr.next();
            }
            O next = itr.next();
            initial = aggregateFunction.reduce(initial, next);
          }
        } else {
          throw new IllegalArgumentException("Invalid Data Type or Reduce Function Type");
        }
        return initial;
      }
    });

    return this;
  }


}
