package edu.iu.dsc.tws.python.tset;

import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.python.processors.PythonLambdaProcessor;

public class PLambdaTSetSource implements SourceFunc {

    private PythonLambdaProcessor processor;
    private Object next;

    public PLambdaTSetSource(byte[] lambda) {
        this.processor = new PythonLambdaProcessor(lambda);
    }

    @Override
    public boolean hasNext() {
        return this.next != null;
    }

    @Override
    public Object next() {
        return next;
    }
}
