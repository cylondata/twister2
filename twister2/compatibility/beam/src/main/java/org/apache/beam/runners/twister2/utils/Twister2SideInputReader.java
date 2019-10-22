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
package org.apache.beam.runners.twister2.utils;

import javax.annotation.Nullable;

import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.twister2.Twister2RuntimeContext;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

public class Twister2SideInputReader implements SideInputReader {

    private final Twister2RuntimeContext runtimeContext;
    private final Map<TupleTag<?>, WindowingStrategy<?, ?>> sideInputs;

    public Twister2SideInputReader(Map<PCollectionView<?>, WindowingStrategy<?, ?>> indexByView, Twister2RuntimeContext context) {
        this.sideInputs = new HashMap<>();

        for (PCollectionView<?> view : indexByView.keySet()) {
            checkArgument(
                    Materializations.MULTIMAP_MATERIALIZATION_URN.equals(
                            view.getViewFn().getMaterialization().getUrn()),
                    "This handler is only capable of dealing with %s materializations "
                            + "but was asked to handle %s for PCollectionView with tag %s.",
                    Materializations.MULTIMAP_MATERIALIZATION_URN,
                    view.getViewFn().getMaterialization().getUrn(),
                    view.getTagInternal().getId());
        }
        for (Map.Entry<PCollectionView<?>, WindowingStrategy<?, ?>> entry : indexByView.entrySet()) {
            sideInputs.put(entry.getKey().getTagInternal(), entry.getValue());
        }
        this.runtimeContext = context;
    }

    @Nullable
    @Override
    public <T> T get(PCollectionView<T> view, BoundedWindow window) {
        checkNotNull(view, "View passed to sideInput cannot be null");
        TupleTag<?> tag = view.getTagInternal();
        checkNotNull(sideInputs.get(tag), "Side input for " + view + " not available.");
        return runtimeContext.<T>getSideInput(window);
    }

    @Override
    public <T> boolean contains(PCollectionView<T> view) {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
