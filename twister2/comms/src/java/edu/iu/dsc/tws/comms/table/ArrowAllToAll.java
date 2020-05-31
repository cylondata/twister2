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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.common.table.ArrowColumn;
import edu.iu.dsc.tws.common.table.Table;
import edu.iu.dsc.tws.common.table.arrow.ArrowTableImpl;
import edu.iu.dsc.tws.common.table.arrow.Float4Column;
import edu.iu.dsc.tws.common.table.arrow.Float8Column;
import edu.iu.dsc.tws.common.table.arrow.Int4Column;
import edu.iu.dsc.tws.common.table.arrow.Int8Column;
import edu.iu.dsc.tws.common.table.arrow.StringColumn;
import edu.iu.dsc.tws.common.table.arrow.TableRuntime;
import edu.iu.dsc.tws.common.table.arrow.UInt2Column;
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

    private List<ArrowBuf> buffers = new ArrayList<>();
    private List<ArrowFieldNode> fieldNodes = new ArrayList<>();
    private List<FieldVector> arrays = new ArrayList<>();
  }

  private class PendingSendTable {
    private int target;
    private Queue<Table> pending = new LinkedList<>();
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
  private VectorSchemaRoot schemaRoot;

  public ArrowAllToAll(Config cfg, IWorkerController controller,
                       Set<Integer> srcs, Set<Integer> targets, int edgeId,
                       ArrowCallback callback, Schema schema) {
    this.targets = new ArrayList<>(targets);
    this.srcs = new ArrayList<>(srcs);
    this.workerId = controller.getWorkerInfo().getWorkerID();
    this.recvCallback = callback;
    TableRuntime runtime = WorkerEnvironment.getSharedValue(TableRuntime.TABLE_RUNTIME_CONF,
        TableRuntime.class);
    assert runtime != null;

    for (int t : targets) {
      inputs.put(t, new PendingSendTable());
    }

    for (int s : srcs) {
      receives.put(s, new PendingReceiveTable());
    }

    this.schemaRoot = VectorSchemaRoot.create(schema, runtime.getRootAllocator());

    this.all = new SimpleAllToAll(cfg, controller, this.srcs, this.targets, edgeId, this,
        new ArrowAllocator(runtime.getRootAllocator()));
  }

  public boolean insert(Table table, int target) {
    PendingSendTable st = inputs.get(target);
    st.pending.offer(table);
    return true;
  }

  /**
   * Check weather complete
   * @return true if operation is complete
   */
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
        List<ArrowColumn> columns = new ArrayList<>();
        // create the table
        for (FieldVector v : fieldVectors) {
          ArrowColumn c;
          if (v instanceof BaseFixedWidthVector) {
            if (v instanceof IntVector) {
              c = new Int4Column((IntVector) v);
            } else if (v instanceof Float4Vector) {
              c = new Float4Column((Float4Vector) v);
            } else if (v instanceof Float8Vector) {
              c = new Float8Column((Float8Vector) v);
            } else if (v instanceof UInt8Vector) {
              c = new Int8Column((UInt8Vector) v);
            } else if (v instanceof UInt2Vector) {
              c = new UInt2Column((UInt2Vector) v);
            } else {
              throw new RuntimeException("Un-supported type : " + v.getClass().getName());
            }
          } else if (v instanceof BaseVariableWidthVector) {
            if (v instanceof VarCharVector) {
              c = new StringColumn((VarCharVector) v);
            } else {
              throw new RuntimeException("Un-supported type : " + v.getClass().getName());
            }
          } else {
            throw new RuntimeException("Un-supported type : " + v.getClass().getName());
          }
          columns.add(c);
        }

        Table t = new ArrowTableImpl(schemaRoot.getSchema(), 0, columns);
        recvCallback.onReceive(source, t);
      }
    }
  }

  @Override
  public void onReceiveHeader(int source, boolean fin, int[] header, int length) {
    if (!fin) {
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
    } else {
      finishedSources.add(source);
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
    checkArgument(nodes.hasNext(), "no more field nodes for for field %s and vector %s",
        field, vector);
    ArrowFieldNode fieldNode = nodes.next();
    int bufferLayoutCount = TypeLayout.getTypeBufferCount(field.getType());
    List<ArrowBuf> ownBuffers = new ArrayList<>(bufferLayoutCount);
    for (int j = 0; j < bufferLayoutCount; j++) {
      ownBuffers.add(buffers.next());
    }
    try {
      vector.loadFieldBuffers(fieldNode, ownBuffers);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Could not load buffers for field "
          + field + ". error message: " + e.getMessage(), e);
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
