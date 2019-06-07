//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

package edu.iu.dsc.tws.api.task.ops;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.task.graph.Edge;

public class BroadcastConfig extends AbstractOpsConfig<BroadcastConfig> {

  BroadcastConfig(String source, ComputeConnection computeConnection) {
    super(source, OperationNames.BROADCAST, computeConnection);
  }

  @Override
  protected Edge updateEdge(Edge newEdge) {
    return newEdge;
  }

  @Override
  void validate() {
    // nothing to do here in broadcast
  }
}
