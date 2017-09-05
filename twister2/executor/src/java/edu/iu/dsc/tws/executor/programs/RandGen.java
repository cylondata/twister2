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
package edu.iu.dsc.tws.executor.programs;

import java.util.Random;

/**
 * Created by vibhatha on 9/5/17.
 */
public class RandGen {
  public static void main(String[] args) {
    Random r = new Random();
    System.out.println("Starting RandGen");
    System.out.println("===========================");
    for (int i = 0; i < 10; i++) {
      System.out.println(r.nextInt(1000));
    }
    System.out.println("End RandGen");
    System.out.println("===========================");
  }

}
