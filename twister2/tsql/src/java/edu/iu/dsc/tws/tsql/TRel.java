package edu.iu.dsc.tws.tsql;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

public interface TRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("TWISTER", TRel.class);
}
