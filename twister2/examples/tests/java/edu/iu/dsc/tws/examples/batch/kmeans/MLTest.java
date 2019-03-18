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
package edu.iu.dsc.tws.examples.batch.kmeans;

import java.util.Arrays;

import org.junit.Test;

import edu.iu.dsc.tws.examples.ml.svm.util.DataGenerator;

public class MLTest {

  @Test
  public void testDataGen() {
    double[] ar = DataGenerator.seedDoubleArray(10);
    System.out.println(Arrays.toString(ar));
  }

  @Test
  public void testHello() {
    System.out.println("Hello");
  }
}
