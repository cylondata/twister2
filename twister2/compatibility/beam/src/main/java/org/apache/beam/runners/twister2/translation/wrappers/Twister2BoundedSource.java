/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.twister2.translation.wrappers;

import static org.apache.beam.vendor.grpc.v1p21p0.io.opencensus.internal.Utils.checkState;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.twister2.Twister2TranslationContext;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @param <T> */
public class Twister2BoundedSource<T> extends BaseSourceFunc<WindowedValue<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(Twister2BoundedSource.class);

  private final BoundedSource<T> source;
  private int numPartitions;
  private long splitSize = 100;
  private transient Config twister2Config;
  List<? extends Source<T>> partitionedSources;
  private Source<T> localPartition;
  private final transient PipelineOptions options;
  private transient Iterator<WindowedValue<T>> readerIterator;
  private static final long DEFAULT_BUNDLE_SIZE = 64L * 1024L * 1024L;

  public Twister2BoundedSource(
      BoundedSource<T> boundedSource, Twister2TranslationContext context, PipelineOptions options) {
    source = boundedSource;
    this.options = options;
  }

  @Override
  public void prepare(TSetContext context) {

    numPartitions = context.getParallelism();

    try {
      splitSize = source.getEstimatedSizeBytes(options) / numPartitions;
    } catch (Exception e) {
      LOG.warn(
          "Failed to get estimated bundle size for source {}, using default bundle "
              + "size of {} bytes.",
          source,
          DEFAULT_BUNDLE_SIZE);
    }
    twister2Config = context.getConfig();
    int index = context.getIndex();
    // checkArgument(this.numPartitions > 0, "Number of partitions must be greater than zero.");

    try {
      partitionedSources = source.split(splitSize, options);
      if (partitionedSources.size() > numPartitions) {
        LOG.warn("Number of partitions is larger then the parallism");
      }
      // todo make sure the partitions are balanced.
      localPartition = partitionedSources.get(index);
      final BoundedSource.BoundedReader<T> reader = createReader(localPartition);

      readerIterator = new ReaderToIteratorAdapter<>(reader);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create partitions for source " + source.getClass().getSimpleName(), e);
    }
  }

  @Override
  public boolean hasNext() {
    return readerIterator.hasNext();
  }

  @Override
  public WindowedValue<T> next() {
    return readerIterator.next();
  }

  private BoundedSource.BoundedReader<T> createReader(Source<T> partition) {
    try {
      return ((BoundedSource<T>) partition).createReader(options);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create reader from a BoundedSource.", e);
    }
  }

  /**
   * Exposes an <code>Iterator</code>&lt;{@link WindowedValue}&gt; interface on top of a {@link
   * Source.Reader}.
   *
   * <p><code>hasNext</code> is idempotent and returns <code>true</code> iff further items are
   * available for reading using the underlying reader. Consequently, when the reader is closed, or
   * when the reader has no further elements available (i.e, {@link Source.Reader#advance()}
   * returned <code>false</code>), <code>hasNext</code> returns <code>false</code>.
   *
   * <p>Since this is a read-only iterator, an attempt to call <code>remove</code> will throw an
   * <code>UnsupportedOperationException</code>.
   */
  static class ReaderToIteratorAdapter<T> implements Iterator<WindowedValue<T>> {

    private static final boolean FAILED_TO_OBTAIN_NEXT = false;
    private static final boolean SUCCESSFULLY_OBTAINED_NEXT = true;

    private final Source.Reader<T> reader;

    private boolean started = false;
    private boolean closed = false;
    private WindowedValue<T> next = null;

    ReaderToIteratorAdapter(final Source.Reader<T> reader) {
      this.reader = reader;
    }

    private boolean tryProduceNext() {
      try {
        checkState(next == null, "unexpected non-null value for next");
        if (seekNext()) {
          next =
              WindowedValue.timestampedValueInGlobalWindow(
                  reader.getCurrent(), reader.getCurrentTimestamp());
          return SUCCESSFULLY_OBTAINED_NEXT;
        } else {
          close();
          return FAILED_TO_OBTAIN_NEXT;
        }

      } catch (final Exception e) {
        throw new RuntimeException("Failed to read data.", e);
      }
    }

    private void close() {
      closed = true;
      try {
        reader.close();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }

    private boolean seekNext() throws IOException {
      if (!started) {
        started = true;
        return reader.start();
      } else {
        return !closed && reader.advance();
      }
    }

    private WindowedValue<T> consumeCurrent() {
      if (next == null) {
        throw new NoSuchElementException();
      } else {
        final WindowedValue<T> current = next;
        next = null;
        return current;
      }
    }

    private WindowedValue<T> consumeNext() {
      if (next == null) {
        tryProduceNext();
      }
      return consumeCurrent();
    }

    @Override
    public boolean hasNext() {
      return next != null || tryProduceNext();
    }

    @Override
    public WindowedValue<T> next() {
      return consumeNext();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
