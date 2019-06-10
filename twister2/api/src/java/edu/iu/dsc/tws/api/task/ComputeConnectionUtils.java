//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
package edu.iu.dsc.tws.api.task;

import edu.iu.dsc.tws.task.graph.Edge;

public final class ComputeConnectionUtils {

  private ComputeConnectionUtils() {

  }

  public static void connectEdge(ComputeConnection computeConnection,
                                 String source,
                                 Edge edge) {
    computeConnection.putEdgeFromSource(source, edge);
  }
}
