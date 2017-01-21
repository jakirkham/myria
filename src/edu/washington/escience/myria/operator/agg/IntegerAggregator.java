package edu.washington.escience.myria.operator.agg;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.math.LongMath;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.DoubleColumn;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.ReplaceableColumn;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public final class IntegerAggregator extends PrimitiveAggregator {

  protected IntegerAggregator(
      final String inputName, final int column, final AggregationOp aggOp, final int[] stateCols) {
    super(inputName, column, aggOp, stateCols);
  }

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  @Override
  public void addRow(
      final ReadableTable from, final int fromRow, final MutableTupleBuffer to, final int toRow) {
    ReadableColumn fromCol = from.asColumn(column);
    ReplaceableColumn toCol = to.getColumn(stateCols[0], toRow);
    switch (aggOp) {
      case COUNT:
        toCol.replaceLong(toCol.getLong(toRow) + 1, toRow);
        break;
      case MAX:
        toCol.replaceInt(Math.max(fromCol.getInt(fromRow), toCol.getInt(toRow)), toRow);
        break;
      case MIN:
        toCol.replaceInt(Math.min(fromCol.getInt(fromRow), toCol.getInt(toRow)), toRow);
        break;
      case SUM:
        toCol.replaceLong(
            LongMath.checkedAdd(fromCol.getInt(fromRow), toCol.getLong(toRow)), toRow);
        break;
      case SUM_SQUARED:
        toCol.replaceLong(
            LongMath.checkedAdd(
                LongMath.checkedMultiply(fromCol.getInt(fromRow), fromCol.getInt(fromRow)),
                toCol.getLong(toRow)),
            toRow);
        break;
      default:
        throw new IllegalArgumentException(aggOp + " is invalid");
    }
  }

  @Override
  public List<Column<?>> emitOutput(final TupleBatch tb) {
    switch (aggOp) {
      case COUNT:
      case SUM:
      case MAX:
      case MIN:
        return (List<Column<?>>) ImmutableList.of(tb.getDataColumns().get(stateCols[0]));
      case AVG:
        {
          ReadableColumn sumCol = tb.asColumn(stateCols[0]);
          ReadableColumn countCol = tb.asColumn(stateCols[1]);
          double[] ret = new double[sumCol.size()];
          for (int i = 0; i < sumCol.size(); ++i) {
            ret[i] = ((double) sumCol.getLong(i)) / countCol.getLong(i);
          }
          List<Column<?>> c = new ArrayList<Column<?>>();
          c.add(new DoubleColumn(ret, ret.length));
          return c;
        }
      case STDEV:
        {
          ReadableColumn sumCol = tb.asColumn(stateCols[0]);
          ReadableColumn sumSquaredCol = tb.asColumn(stateCols[1]);
          ReadableColumn countCol = tb.asColumn(stateCols[2]);
          double[] ret = new double[sumCol.size()];
          for (int i = 0; i < sumCol.size(); ++i) {
            double first = ((double) sumSquaredCol.getLong(i)) / countCol.getLong(i);
            double second = ((double) sumCol.getLong(i)) / countCol.getLong(i);
            ret[i] = Math.sqrt(first - second * second);
          }
          List<Column<?>> c = new ArrayList<Column<?>>();
          c.add(new DoubleColumn(ret, ret.length));
          return c;
        }
      default:
        throw new IllegalArgumentException(aggOp + " is invalid");
    }
  }

  @Override
  protected boolean isSupported(final AggregationOp aggOp) {
    return true;
  }

  @Override
  protected Type getOutputType() {
    switch (aggOp) {
      case COUNT:
      case SUM:
        return Type.LONG_TYPE;
      case MAX:
      case MIN:
        return Type.INT_TYPE;
      case AVG:
      case STDEV:
        return Type.DOUBLE_TYPE;
      default:
        throw new IllegalArgumentException("Type " + aggOp + " is invalid");
    }
  };

  @Override
  public void appendInitValue(final AppendableTable data, final int column) {
    switch (aggOp) {
      case COUNT:
      case SUM:
      case SUM_SQUARED:
        data.putLong(column, 0);
        break;
      case MAX:
        data.putInt(column, Integer.MIN_VALUE);
        break;
      case MIN:
        data.putInt(column, Integer.MAX_VALUE);
        break;
      default:
        throw new IllegalArgumentException("Type " + aggOp + " is invalid");
    }
  }
}
