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
package edu.iu.dsc.tws.examples.comms.batch;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.common.table.Table;
import edu.iu.dsc.tws.common.table.arrow.ArrowTable;
import edu.iu.dsc.tws.comms.table.ArrowAllToAll;
import edu.iu.dsc.tws.comms.table.ArrowCallback;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;
import edu.iu.dsc.tws.examples.comms.JobParameters;

public class BTAllToAll implements IWorker {
  private static final Logger LOG = Logger.getLogger(BTAllToAll.class.getName());

  private ArrowAllToAll allToAll;

  private WorkerEnvironment wEnv;

  protected JobParameters jobParameters;

  @Override
  public void execute(Config config, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    this.jobParameters = JobParameters.build(config);

    // create a worker environment
    this.wEnv = WorkerEnvironment.init(config, workerID, workerController, persistentVolume,
        volatileVolume);

    LogicalPlanBuilder logicalPlanBuilder = LogicalPlanBuilder.plan(
        jobParameters.getSources(),
        jobParameters.getTargets(),
        wEnv
    ).withFairDistribution();

    RootAllocator rootAllocator = new RootAllocator();
    IntVector intVector = new IntVector("fist", rootAllocator);
    Float8Vector float8Vector = new Float8Vector("second", rootAllocator);
    for (int i = 0; i < 1000; i++) {
      intVector.setSafe(i, i);
      float8Vector.setSafe(i, i);
    }

    List<Field> fieldList = Arrays.asList(intVector.getField(), float8Vector.getField());
    Schema schema = new Schema(fieldList);
    Table t = new ArrowTable(schema, Arrays.asList(new FieldVector[]{intVector, float8Vector}));


    allToAll = new ArrowAllToAll(wEnv.getConfig(), wEnv.getWorkerController(),
        logicalPlanBuilder.getSources(), logicalPlanBuilder.getTargets(),
        logicalPlanBuilder.build(), wEnv.getCommunicator().nextEdge(), new ArrowReceiver(),
        schema, rootAllocator);
    for (int i : logicalPlanBuilder.getTargets()) {
      allToAll.insert(t, i);
    }

    for (int s : logicalPlanBuilder.getSourcesOnThisWorker()) {
      allToAll.finish(s);
    }

    while (allToAll.isComplete()) {
      // wait
    }
  }

  private static class ArrowReceiver implements ArrowCallback {
    @Override
    public void onReceive(int source, int target, Table table) {
      LOG.info("Received table to source " + source);
    }
  }
}
