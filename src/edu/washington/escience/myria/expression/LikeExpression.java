package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.SimplePredicate;

/**
 * Comparison for greater than in expression tree.
 */
public class LikeExpression extends ComparisonExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is used automagically by Jackson deserialization.
   */
  private LikeExpression() {
    super(SimplePredicate.Op.LIKE);
  }

  /**
   * True if left > right.
   *
   * @param left the left operand.
   * @param right the right operand.
   */
  public LikeExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right, SimplePredicate.Op.LIKE);
  }
}
