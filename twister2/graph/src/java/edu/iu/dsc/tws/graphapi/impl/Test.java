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
package edu.iu.dsc.tws.graphapi.impl;


import java.io.Serializable;
import java.util.ArrayList;


import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.graphapi.api.BasicComputation;


public class Test extends BasicComputation implements Serializable {

  private PageRankSource pageRankSource = new Task1();
  private PageRankKeyedReduce pageRankKeyedReduce = new Task2();


  @Override
  public ComputeGraph computation() {
    return buildComputationTG(parallelism, config,
        pageRankSource, pageRankKeyedReduce);

  }

  public class Task1 extends PageRankSource {


    @Override
    public void sendmessage(String edgename, ArrayList<String> nearestVertex, double pageValue) {
      int arraySize = nearestVertex.size();
      for (int j = 0; j < arraySize; j++) {
        Double dividedvalue = pageValue / arraySize;
        writemessage(edgename, nearestVertex.get(j), dividedvalue);
      }

    }
  }
  public class Task2 extends PageRankKeyedReduce {

    @Override
    public double calculation(double sumedvalue) {
      double pagerankValue  = (0.15 / graphsize) + (0.85 * sumedvalue);
      return pagerankValue;
    }
  }


}
