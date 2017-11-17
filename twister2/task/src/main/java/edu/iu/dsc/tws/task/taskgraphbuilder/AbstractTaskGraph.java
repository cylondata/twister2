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
package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.util.Set;

/***
 *
 * @param <TV>
 * @param <TE>
 */
public abstract class AbstractTaskGraph<TV, TE> implements TaskGraph<TV, TE> {

    private TaskEdgeFactory<TV, TE> taskEdgeFactory;

    public AbstractTaskGraph(TaskEdgeFactory<TV, TE> taskEdgeFactory){

        this.taskEdgeFactory = taskEdgeFactory;
    }

    @Override
    public Set<TE> getAllTaskEdges(TV taskVertex1, TV taskVertex2) {
        return null;
    }

    @Override
    public TE getTaskEdge(TV taskVertex1, TV taskVertex2) {
        return null;
    }

    @Override
    public boolean addTaskEdge(TV taskVertex1, TV taskVertex2, TE taskEdge) {
        return false;
    }

    @Override
    public boolean addTaskVertex(TV taskVertex) {
        return false;
    }

    @Override
    public boolean containsTaskEdge(TV taskVertex1, TV taskVertex2) {
        return false;
    }

    @Override
    public boolean containsTaskEdge(TE taskEdge) {
        return false;
    }

    @Override
    public boolean containsTaskVertex(TV taskVertex) {
        return false;
    }

    @Override
    public TE addTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {
        return null;
    }

    @Override
    public TaskEdgeFactory<TV, TE> getTaskEdgeFactory() {
        return taskEdgeFactory;
    }

    @Override
    public Set<TV> taskVertexSet() {
        return null;
    }

    @Override
    public Set<TE> taskEdgeSet() {
        return null;
    }

    @Override
    public TE removeTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {
        return null;
    }

    @Override
    public Set<TE> removeAllTaskEdges(TV sourceTaskVertex, TV targetTaskVertex) {
        return null;
    }

    @Override
    public boolean removeTaskVertex(TV taskVertex) {
        return false;
    }


}
