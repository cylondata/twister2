package edu.iu.dsc.tws.python.tset;

import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.python.processors.PythonClassProcessor;

public class PyTSetSource implements SourceFunc {

    private PythonClassProcessor processor;

    public PyTSetSource(byte[] lambda) {
        this.processor = new PythonClassProcessor(lambda);
    }

    @Override
    public boolean hasNext() {
        return (Boolean) this.processor.invoke("has_next");
    }

    @Override
    public Object next() {
        return this.processor.invoke("next");
    }
}
