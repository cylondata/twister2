package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class ArraySet<TE> extends ArrayList<TE> implements Set<TE> {

  private static final long serialVersionUID = 4323424243432434234L;

  public ArraySet() {
    super();
  }

  public ArraySet(Collection<? extends TE> c) {
    super(c);
  }

  public ArraySet(int n) {
    super(n);
  }

  @Override
  public boolean equals(Object o) {
    return new SetForEquality().equals(o);
  }

  @Override
  public int hashCode() {
    return new SetForEquality().hashCode();
  }

  private class SetForEquality extends AbstractSet<TE> {

    public Iterator<TE> iterator() {
      return ArraySet.this.iterator();
    }

    public int size() {
      return ArraySet.this.size();
    }
  }
}

