package group.bda.federate.sql.operator;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexProgram;

import group.bda.federate.data.Header;
import group.bda.federate.sql.expression.FedSpatialExpressions;

public class FederateCalc extends Calc implements FedSpatialRel {

  public FederateCalc(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints, RelNode child, RexProgram program) {
    super(cluster, traits, hints, child, program);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    RelNode child = ((RelSubset)getInput()).getBest();
    if (child instanceof FederateAggregate) {
      return planner.getCostFactory().makeInfiniteCost();
    }
    return super.computeSelfCost(planner, mq).multiplyBy(0.5);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    Header header = implementor.getHeader();
    RexProgram program = getProgram();
    assert header.size() == program.getInputRowType().getFieldCount();
    FedSpatialExpressions projects = FedSpatialExpressions.create(header, program);
    implementor.setSelectExps(projects);
  }

  @Override
  public Calc copy(RelTraitSet traits, RelNode child, RexProgram program) {
    return new FederateCalc(getCluster(), traits, getHints(), child, program);
  }
}
