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
package edu.iu.dsc.tws.examples.verification;

import java.util.Map;
import java.util.logging.Logger;

public class ResultsVerifier<I, O> {

  private static final Logger LOG = Logger.getLogger(ResultsVerifier.class.getName());

  private I input;
  private ResultsGenerator<I, O> resultsGenerator;
  private ResultsComparator<O> resultsComparator;

  public ResultsVerifier(I input,
                         ResultsGenerator<I, O> resultsGenerator,
                         ResultsComparator<O> resultsComparator) {
    this.input = input;
    this.resultsGenerator = resultsGenerator;
    this.resultsComparator = resultsComparator;
  }

  public ResultsVerifier(I input, ResultsGenerator<I, O> resultsGenerator) {
    this.input = input;
    this.resultsGenerator = resultsGenerator;

    //use default comparator. Only good for objects with equal overridden
    this.resultsComparator = Object::equals;
  }

  public boolean verify(O generatedOutput, Map<String, Object> args) {
    O expectedOutput = this.resultsGenerator.generateResults(this.input, args);
    boolean equal = this.resultsComparator.compare(expectedOutput, generatedOutput);
    if (!equal) {
      LOG.info("Results verification failed!"
          + "\n\tExpected : " + expectedOutput.toString()
          + "\n\tFound : " + generatedOutput.toString());
    } else {
      LOG.info("Results are verified!");
    }
    return equal;
  }
}
