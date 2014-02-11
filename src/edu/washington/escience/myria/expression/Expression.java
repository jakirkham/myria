package edu.washington.escience.myria.expression;

import java.io.Serializable;
import java.util.LinkedList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.builder.ColumnBuilder;

/**
 * An expression that can be applied to a tuple.
 */
public class Expression implements Serializable {

  /***/
  private static final long serialVersionUID = 1L;

  /**
   * Name of the column that the result will be written to.
   */
  @JsonProperty
  private final String outputName;

  /**
   * The java expression to be evaluated.
   */
  @JsonProperty
  private String javaExpression;

  /**
   * Expression encoding reference is needed to get the output type.
   */
  @JsonProperty
  private final ExpressionOperator rootExpressionOperator;

  /**
   * Variable name of result.
   */
  public static final String RESULT = "result";
  /**
   * Variable name of input tuple batch.
   */
  public static final String TB = "tb";
  /**
   * Variable name of row index.
   */
  public static final String ROW = "row";
  /**
   * Variable name of state.
   */
  public static final String STATE = "state";

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  public Expression() {
    outputName = null;
    rootExpressionOperator = null;
  }

  /**
   * Constructs the Expression object.
   * 
   * @param rootExpressionOperator the root of the AST representing this expression.
   */
  public Expression(final ExpressionOperator rootExpressionOperator) {
    this.rootExpressionOperator = rootExpressionOperator;
    outputName = null;
  }

  /**
   * Constructs the Expression object.
   * 
   * @param outputName the name of the resulting element
   * @param rootExpressionOperator the root of the AST representing this expression.
   */
  public Expression(final String outputName, final ExpressionOperator rootExpressionOperator) {
    this.rootExpressionOperator = rootExpressionOperator;
    this.outputName = outputName;
  }

  /**
   * @return the rootExpressionOperator
   */
  public ExpressionOperator getRootExpressionOperator() {
    return rootExpressionOperator;
  }

  /**
   * @return the output name
   */
  public String getOutputName() {
    return outputName;
  }

  /**
   * @param inputSchema the schema of the input relation
   * @param stateSchema the schema of the state
   * @return the Java form of this expression.
   */
  public String getJavaExpression(final Schema inputSchema, final Schema stateSchema) {
    if (javaExpression == null) {
      return rootExpressionOperator.getJavaString(inputSchema, stateSchema);
    }
    return javaExpression;
  }

  /**
   * @param inputSchema the schema of the input relation
   * @param stateSchema the schema of the state
   * @return the type of the output
   */
  public Type getOutputType(final Schema inputSchema, final Schema stateSchema) {
    return rootExpressionOperator.getOutputType(inputSchema, stateSchema);
  }

  /**
   * @return the Java form of this expression.
   */
  public String getJavaExpression() {
    if (javaExpression == null) {
      return rootExpressionOperator.getJavaString(null, null);
    }
    return javaExpression;
  }

  /**
   * @param inputSchema the schema of the input relation
   * @param stateSchema the schema of the state
   * @return the Java form of this expression that also writes the results to a {@link ColumnBuilder}.
   */
  public String getJavaExpressionWithAppend(final Schema inputSchema, final Schema stateSchema) {
    return new StringBuilder(RESULT).append(".append").append(getOutputType(inputSchema, stateSchema).getName())
        .append("(").append(getJavaExpression(inputSchema, stateSchema)).append(")").toString();
  }

  /**
   * Reset {@link #javaExpression}.
   */
  public void resetJavaExpression() {
    javaExpression = null;
  }

  /**
   * @param optype Class to find
   * @return true if the operator is in the expression
   */
  public boolean hasOperator(final Class<?> optype) {
    LinkedList<ExpressionOperator> ops = Lists.newLinkedList();
    ops.add(getRootExpressionOperator());
    while (!ops.isEmpty()) {
      final ExpressionOperator op = ops.pop();
      if (op.getClass().equals(optype)) {
        return true;
      }
      ops.addAll(op.getChildren());
    }
    return false;
  }

  /**
   * @return if this expression evaluates to a constant
   */
  public boolean isConstant() {
    return !hasOperator(VariableExpression.class) && !hasOperator(StateExpression.class);
  }
}