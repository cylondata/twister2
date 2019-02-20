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

import java.util.logging.Logger;

public class ResultsVerifier<I, O extends Comparable<O>> {

  private static final Logger LOG = Logger.getLogger(ResultsVerifier.class.getName());

  private I input;
  private ResultsGenerator<I, O> resultsGenerator;

  public ResultsVerifier(I input, ResultsGenerator<I, O> resultsGenerator) {
    this.input = input;
    this.resultsGenerator = resultsGenerator;
  }

  public boolean verify(O generatedOutput) {
    O expectedOutput = this.resultsGenerator.generateResults(this.input);
    int comparison = this.resultsGenerator.generateResults(this.input)
        .compareTo(generatedOutput);
    if (comparison != 0) {
      LOG.info("Generated output and expected output doesn't match. "
          + "Expected : " + expectedOutput.toString()
          + "\nFound : " + generatedOutput.toString());
    }
    return comparison == 0;
  }
}
