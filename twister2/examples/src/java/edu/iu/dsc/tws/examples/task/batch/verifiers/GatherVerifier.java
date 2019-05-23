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
package edu.iu.dsc.tws.examples.task.batch.verifiers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.examples.comms.JobParameters;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;
import edu.iu.dsc.tws.examples.verification.comparators.IntComparator;
import edu.iu.dsc.tws.examples.verification.comparators.IteratorComparator;
import edu.iu.dsc.tws.examples.verification.comparators.TupleComparator;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;

public class GatherVerifier extends ResultsVerifier<int[], Iterator<Tuple<Integer, int[]>>> {

  public GatherVerifier(int[] inputDataArray, TaskContext ctx,
                        String sourceTaskName, JobParameters jobParameters) {
    super(
        inputDataArray, (ints, args) -> {
          Set<Integer> taskIds = ctx.getTasksByName(sourceTaskName).stream()
              .map(TaskInstancePlan::getTaskIndex)
              .collect(Collectors.toSet());
          int taskId = ctx.getTasksByName(sourceTaskName).stream().findFirst().get().getTaskId();
          List<Tuple<Integer, int[]>> generatedData = new ArrayList<>();
          for (Integer taskIndex : taskIds) {
            for (int i = 0; i < jobParameters.getTotalIterations(); i++) {
              //todo temp 100000, change once engine is fixed
              generatedData.add(new Tuple<>((taskId * 100000) + taskIndex, ints));
            }
          }
          return generatedData.iterator();
        }, new IteratorComparator<>(
            new TupleComparator<>(
                IntComparator.getInstance(),
                IntArrayComparator.getInstance()
            )
        )
    );
  }
}
