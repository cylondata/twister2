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
package edu.iu.dsc.tws.comms.table;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.common.table.ArrowColumn;
import edu.iu.dsc.tws.common.table.Table;
import edu.iu.dsc.tws.common.table.arrow.TableRuntime;
import edu.iu.dsc.tws.comms.table.channel.ChannelBuffer;
import io.netty.buffer.ArrowBuf;
import static org.apache.arrow.util.Preconditions.checkArgument;

/**
 * This class gets values as normal format and converts them into a row format before invoking the
 * communication.
 */
public class ArrowAllToAll implements ReceiveCallback {
  private static final Logger LOG = Logger.getLogger(ArrowAllToAll.class.getName());

  private enum ArrowHeader {
    HEADER_INIT,
    COLUMN_CONTINUE
  }

  private class PendingReceiveTable {
    private int source;
    private int columnIndex;
    private int bufferIndex;
    private int noBuffers;
    private int noArray;
    private int length;

    private List<ArrowBuf> buffers;
    private List<ArrowFieldNode> fieldNodes;
    private List<FieldVector> arrays;
  }

  private class PendingSendTable {
    private int target;
    private Queue<Table> pending;
    private Table currentTable;
    private ArrowHeader status = ArrowHeader.HEADER_INIT;
    private int columnIndex;
    private int bufferIndex;
  }

  private List<Integer> targets;
  private List<Integer> srcs;
  private SimpleAllToAll all;
  private Map<Integer, PendingSendTable> inputs = new HashMap<>();
  private Map<Integer, PendingReceiveTable> receives = new HashMap<>();
  private ArrowCallback recvCallback;
  private boolean finished = false;
  private List<Integer> finishedSources = new ArrayList<>();
  private int receivedBuffers;
  private int workerId;
  private TableRuntime runtime;
  private Schema schema;
  private VectorSchemaRoot schemaRoot;

  public ArrowAllToAll(Config cfg, IWorkerController controller,
                       List<Integer> srcs, List<Integer> targets, int edgeId,
                       ArrowCallback callback, Schema schema) {
    this.targets = targets;
    this.srcs = srcs;
    this.workerId = controller.getWorkerInfo().getWorkerID();
    this.recvCallback = callback;
    this.schema = schema;
    this.runtime = WorkerEnvironment.getSharedValue(TableRuntime.TABLE_RUNTIME_CONF,
        TableRuntime.class);
    assert runtime != null;

    for (int t : targets) {
      inputs.put(t, new PendingSendTable());
    }

    for (int s : srcs) {
      receives.put(s, new PendingReceiveTable());
    }

    this.all = new SimpleAllToAll(cfg, controller, srcs, targets, edgeId, this,
        new ArrowAllocator(this.runtime.getRootAllocator()));
  }

  public boolean insert(Table table, int target) {
    PendingSendTable st = inputs.get(target);
    st.pending.offer(table);
    return true;
  }

  public boolean isComplete() {
    boolean isAllEmpty = true;

    for (Map.Entry<Integer, PendingSendTable> t : inputs.entrySet()) {
      PendingSendTable pst = t.getValue();
      if (pst.status == ArrowHeader.HEADER_INIT) {
        if (!pst.pending.isEmpty()) {
          pst.currentTable = pst.pending.peek();
          pst.pending.poll();
          pst.status = ArrowHeader.COLUMN_CONTINUE;
        }
      }

      if (pst.status == ArrowHeader.COLUMN_CONTINUE) {
        int noOfColumns = pst.currentTable.getColumns().size();
        boolean canContinue = true;
        while (pst.columnIndex < noOfColumns && canContinue) {
          ArrowColumn col = pst.currentTable.getColumns().get(pst.columnIndex);
          FieldVector vector = col.getVector();

          List<ArrowFieldNode> nodes = new ArrayList<>();
          List<ArrowBuf> bufs = new ArrayList<>();
          appendNodes(vector, nodes, bufs);

          while (pst.bufferIndex < bufs.size()) {
            ArrowBuf buf = bufs.get(pst.bufferIndex);
            int[] hdr = new int[5];
            hdr[0] = pst.columnIndex;
            hdr[1] = pst.bufferIndex;
            hdr[2] = bufs.size();
            hdr[3] = 1;
            int length = nodes.get(pst.bufferIndex).getLength();
            hdr[4] = length;

            boolean accept = all.insert(buf.nioBuffer(), length, hdr, 5, t.getKey());
            if (!accept) {
              canContinue = false;
              break;
            }
            pst.bufferIndex++;
          }

          if (canContinue) {
            pst.bufferIndex = 0;
            pst.columnIndex++;
          }
        }

        if (canContinue) {
          pst.columnIndex = 0;
          pst.bufferIndex = 0;
          pst.status = ArrowHeader.HEADER_INIT;
        }
      }

      if (!pst.pending.isEmpty() || pst.status == ArrowHeader.COLUMN_CONTINUE) {
        isAllEmpty = false;
      }
    }

    if (isAllEmpty && finished) {
      all.finish(0);
    }
    return isAllEmpty && all.isComplete() && finishedSources.size() == srcs.size();
  }

  public void finish(int source) {
    finished = true;
  }

  public void close() {
    inputs.clear();
    all.close();
  }

  @Override
  public void onReceive(int source, ChannelBuffer buffer, int length) {
    PendingReceiveTable table = receives.get(source);
    receivedBuffers++;
    ArrowBuf buf = ((ArrowChannelBuffer) buffer).getArrowBuf();
    table.buffers.add(buf);

    List<FieldVector> fieldVectors = schemaRoot.getFieldVectors();
    // we received everything for this array
    if (table.noBuffers == table.bufferIndex + 1) {
      FieldVector fieldVector = fieldVectors.get(table.columnIndex);
      loadBuffers(fieldVector, fieldVector.getField(), table.buffers.iterator(),
          table.fieldNodes.iterator());

      table.arrays.add(fieldVector);
      table.buffers.clear();

      if (table.arrays.size() == schemaRoot.getFieldVectors().size()) {
        // create the table

      }
    }
  }

  @Override
  public void onReceiveHeader(int source, boolean finished, int[] header, int length) {
    if (!finished) {
      if (length != 5) {
        String msg = "Incorrect length on header, expected 5 ints got " + length;
        LOG.log(Level.SEVERE, msg);
        throw new RuntimeException(msg);
      }

      PendingReceiveTable table = receives.get(source);
      table.columnIndex = header[0];
      table.bufferIndex = header[1];
      table.noBuffers = header[2];
      table.noArray = header[3];
      table.length = header[4];
    }
  }

  @Override
  public boolean onSendComplete(int target, ByteBuffer buffer, int length) {
    return false;
  }

  private void loadBuffers(
      FieldVector vector,
      Field field,
      Iterator<ArrowBuf> buffers,
      Iterator<ArrowFieldNode> nodes) {
    checkArgument(nodes.hasNext(), "no more field nodes for for field %s and vector %s", field, vector);
    ArrowFieldNode fieldNode = nodes.next();
    int bufferLayoutCount = TypeLayout.getTypeBufferCount(field.getType());
    List<ArrowBuf> ownBuffers = new ArrayList<>(bufferLayoutCount);
    for (int j = 0; j < bufferLayoutCount; j++) {
      ownBuffers.add(buffers.next());
    }
    try {
      vector.loadFieldBuffers(fieldNode, ownBuffers);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Could not load buffers for field " +
          field + ". error message: " + e.getMessage(), e);
    }
    List<Field> children = field.getChildren();
    if (children.size() > 0) {
      List<FieldVector> childrenFromFields = vector.getChildrenFromFields();
      checkArgument(children.size() == childrenFromFields.size(),
          "should have as many children as in the schema: found %s expected %s",
          childrenFromFields.size(), children.size());
      for (int i = 0; i < childrenFromFields.size(); i++) {
        Field child = children.get(i);
        FieldVector fieldVector = childrenFromFields.get(i);
        loadBuffers(fieldVector, child, buffers, nodes);
      }
    }
  }

  private void appendNodes(FieldVector vector, List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
    nodes.add(new ArrowFieldNode(vector.getValueCount(), -1));
    List<ArrowBuf> fieldBuffers = vector.getFieldBuffers();
    int expectedBufferCount = TypeLayout.getTypeBufferCount(vector.getField().getType());
    if (fieldBuffers.size() != expectedBufferCount) {
      throw new IllegalArgumentException(String.format(
          "wrong number of buffers for field %s in vector %s. found: %s",
          vector.getField(), vector.getClass().getSimpleName(), fieldBuffers));
    }
    buffers.addAll(fieldBuffers);
    for (FieldVector child : vector.getChildrenFromFields()) {
      appendNodes(child, nodes, buffers);
    }
  }
}
