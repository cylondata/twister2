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

package edu.iu.dsc.tws.api.tset.fn;

/**
 * Computes an output of type O based on an input of type I and the output would be passed on to
 * a {@link RecordCollector} object.
 *
 * @param <O> output type
 * @param <I> input type
 */
public interface ComputeCollectorFunc<I, O> extends TFunction<I, O> {

  void compute(I input, RecordCollector<O> output);
}

