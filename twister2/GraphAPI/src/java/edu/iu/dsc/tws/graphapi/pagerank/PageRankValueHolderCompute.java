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
package edu.iu.dsc.tws.graphapi.pagerank;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.BaseCompute;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;

public class PageRankValueHolderCompute extends BaseCompute {

  private static final Logger LOG = Logger.getLogger(DataObjectCompute.class.getName());
  private static final long serialVersionUID = -254264120110286748L;

  /**
   * Edge name to write the partitoned datapoints
   */
  private String edgeName;

  /**
   * Task parallelism
   */
  private int parallel;

  /**
   * Data size
   */
  private int datasize;



  public PageRankValueHolderCompute(String edgename, int dsize, int parallel) {
    this.edgeName = edgename;
    this.parallel = parallel;
    this.datasize = dsize;
  }

  public PageRankValueHolderCompute(String edgename, int size) {
    this.edgeName = edgename;
    this.datasize = size;
  }

  public int getDatasize() {
    return datasize;
  }

  public void setDatasize(int datasize) {
    this.datasize = datasize;
  }

  public int getParallel() {
    return parallel;
  }

  public void setParallel(int parallel) {
    this.parallel = parallel;
  }

  public String getEdgeName() {

    return edgeName;
  }

  public void setEdgeName(String edgeName) {

    this.edgeName = edgeName;
  }

  @Override
  public boolean execute(IMessage message) {
    if (message.getContent() instanceof Iterator) {
      int dataSize = getDatasize();
      HashMap<String, Double> map = new HashMap<String, Double>();

      while (((Iterator) message.getContent()).hasNext()) {
        String val = String.valueOf(((Iterator) message.getContent()).next());
        String[] data = val.split("\\s+");
        ArrayList<String> adjList = new ArrayList<String>(Arrays.asList(data));
        map.put(data[0], (double) 1 / dataSize);
        context.write(getEdgeName(), map);
      }
    }
    context.end(getEdgeName());
    return true;
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
  }
}
