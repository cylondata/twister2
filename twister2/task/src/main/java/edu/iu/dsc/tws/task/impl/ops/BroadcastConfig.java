//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

package edu.iu.dsc.tws.task.impl.ops;

import edu.iu.dsc.tws.api.comms.OperationNames;
import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.task.impl.ComputeConnection;

public class BroadcastConfig extends AbstractOpsConfig<BroadcastConfig> {

  public BroadcastConfig(String source, ComputeConnection computeConnection) {
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
