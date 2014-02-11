/**
 * 
 */
package edu.washington.escience.myria.expression;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.Schema;

/**
 * An abstract class representing some variable in an expression tree.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    /* Zeroary */
    @Type(name = "CONSTANT", value = ConstantExpression.class), @Type(name = "STATE", value = StateExpression.class),
    @Type(name = "TYPE", value = TypeExpression.class), @Type(name = "TYPEOF", value = TypeOfExpression.class),
    @Type(name = "VARIABLE", value = VariableExpression.class),
    /* Unary */
    @Type(name = "ABS", value = AbsExpression.class), @Type(name = "CAST", value = CastExpression.class),
    @Type(name = "CEIL", value = CeilExpression.class), @Type(name = "COS", value = CosExpression.class),
    @Type(name = "FLOOR", value = FloorExpression.class), @Type(name = "LOG", value = LogExpression.class),
    @Type(name = "NOT", value = NotExpression.class), @Type(name = "NEG", value = NegateExpression.class),
    @Type(name = "SIN", value = SinExpression.class), @Type(name = "SQRT", value = SqrtExpression.class),
    @Type(name = "TAN", value = TanExpression.class), @Type(name = "UPPER", value = ToUpperCaseExpression.class),
    /* Binary */
    @Type(name = "AND", value = AndExpression.class), @Type(name = "DIVIDE", value = DivideExpression.class),
    @Type(name = "EQ", value = EqualsExpression.class), @Type(name = "GT", value = GreaterThanExpression.class),
    @Type(name = "GTEQ", value = GreaterThanOrEqualsExpression.class),
    @Type(name = "LTEQ", value = LessThanOrEqualsExpression.class),
    @Type(name = "LT", value = LessThanExpression.class), @Type(name = "MINUS", value = MinusExpression.class),
    @Type(name = "NEQ", value = NotEqualsExpression.class), @Type(name = "OR", value = OrExpression.class),
    @Type(name = "PLUS", value = PlusExpression.class), @Type(name = "POW", value = PowExpression.class),
    @Type(name = "TIMES", value = TimesExpression.class), })
public abstract class ExpressionOperator implements Serializable {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * @param schema the schema of the tuples this expression references.
   * @param stateSchema the schema of the state if used.
   * @return the type of the output of this expression.
   */
  public abstract edu.washington.escience.myria.Type getOutputType(final Schema schema, final Schema stateSchema);

  /**
   * @param schema the input schema
   * @param stateSchema the schema of the state if used.
   * @return the entire tree represented as an expression.
   */
  public abstract String getJavaString(final Schema schema, final Schema stateSchema);

  /**
   * @return all children
   */
  public abstract List<ExpressionOperator> getChildren();
}