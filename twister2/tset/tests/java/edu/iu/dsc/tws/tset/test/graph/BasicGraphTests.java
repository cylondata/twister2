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
package edu.iu.dsc.tws.tset.test.graph;

import org.junit.Before;
import org.junit.Test;

import edu.iu.dsc.tws.tset.graph.DAGMutableGraph;
import edu.iu.dsc.tws.tset.graph.MutableGraph;

import static org.junit.Assert.*;

public class BasicGraphTests {

  MutableGraph<String> graph;

  @Before
  public void setUp() {
  }

  @Test
  public void addNode() {
    graph = new DAGMutableGraph<>();
    graph.addNode("1");
    assertEquals(1, graph.nodes().size());
    graph.addNode("2");
    assertEquals(2, graph.nodes().size());
    graph.addNode("2");
    assertEquals(2, graph.nodes().size());
  }

  @Test
  public void addEdge(){
    graph = new DAGMutableGraph<>();
    assertTrue(graph.putEdge("1","2"));
    assertFalse(graph.putEdge("1","2"));

    assertTrue(graph.successors("1").contains("2"));
    assertTrue(graph.predecessors("2").contains("1"));
    assertFalse(graph.successors("2").contains("1"));
    assertFalse(graph.predecessors("1").contains("2"));
  }

  @Test(expected = IllegalStateException.class)
  public void checkSelfCycle(){
    graph = new DAGMutableGraph<>();
    graph.addNode("1");
    graph.putEdge("1","2");
    graph.putEdge("1","1");
  }

  @Test(expected = IllegalStateException.class)
  public void checkCycle(){
    graph = new DAGMutableGraph<>();
    graph.putEdge("0","2");
    graph.putEdge("0", "1");
    graph.putEdge("1", "3");
    graph.putEdge("3", "4");
    graph.putEdge("3", "1");
  }
}
