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
package edu.iu.dsc.tws.api.htgjob;

import java.util.logging.Logger;

import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;

public final class Twister2HTGScheduler {

  private static final Logger LOG = Logger.getLogger(Twister2HTGScheduler.class.getName());

  private Twister2HTGScheduler() {
  }


  /**
   * Now, it is in first in first out, we have to modify this.
   */
  public static HTGJobAPI.SubGraph schedule(
      Twister2HTGJob.Twister2HTGMetaGraph twister2HTGJob) {

    HTGJobAPI.HTGJob htgJob = twister2HTGJob.build().serialize();

    LOG.info("%%%%%%%%%%%%%%HTG Subgraphs%%%%%%%%%%%%%%%" + htgJob.getGraphsList());

    HTGJobAPI.SubGraph subGraph = null;

    for (int i = 0; i < htgJob.getGraphsList().size(); i++) {
      subGraph = htgJob.getGraphs(i);
      LOG.info("Sub Graph and its Resource Details:" + subGraph);
    }

    return subGraph;
  }
}
