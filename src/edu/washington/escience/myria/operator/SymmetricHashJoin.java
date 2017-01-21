package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.gs.collections.api.iterator.IntIterator;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.parallel.QueryExecutionMode;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * This is an implementation of hash equal join. The same as in DupElim, this implementation does not keep the
 * references to the incoming TupleBatches in order to get better memory performance.
 */
public final class SymmetricHashJoin extends BinaryOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The names of the output columns. */
  private final ImmutableList<String> outputColumns;
  /** The column indices for comparing of the left child. */
  private int[] leftCompareIndx;
  /** The column indices for comparing of the right child. */
  private int[] rightCompareIndx;
  /** Which columns in the left child are to be output. */
  private int[] leftAnswerColumns;
  /** Which columns in the right child are to be output. */
  private int[] rightAnswerColumns;
  /** The buffer holding the valid tuples from left. */
  public transient TupleHashTable leftHashTable;
  /** The buffer holding the valid tuples from right. */
  public transient TupleHashTable rightHashTable;
  /** The buffer holding the results. */
  private transient TupleBatchBuffer ans;
  /** Whether the last child polled was the left child. */
  private boolean pollLeft = false;
  /** Join pull order, default: ALTER. */
  private JoinPullOrder order = JoinPullOrder.ALTER;
  /** if the hash table of the left child should use set semantics. */
  private boolean setSemanticsLeft = false;
  /** if the hash table of the right child should use set semantics. */
  private boolean setSemanticsRight = false;
  public int[] triggers = new int[] {};

  /**
   * Construct an SymmetricHashJoin operator. It returns the specified columns from both children when the corresponding
   * columns in compareIndx1 and compareIndx2 match.
   *
   * @param outputColumns the names of the columns in the output schema. If null, the corresponding columns will be
   *          copied from the children.
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @param answerColumns1 the columns of the left child to be returned. Order matters.
   * @param answerColumns2 the columns of the right child to be returned. Order matters. * @param setSemanticsLeft if
   *          the hash table of the left child should use set semantics.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputColumns</tt>, or if
   *        <tt>outputColumns</tt> does not have the correct number of columns and column types.
   */
  public SymmetricHashJoin(
      final Operator left,
      final Operator right,
      final int[] compareIndx1,
      final int[] compareIndx2,
      final int[] answerColumns1,
      final int[] answerColumns2) {
    this(
        left,
        right,
        compareIndx1,
        compareIndx2,
        answerColumns1,
        answerColumns2,
        false,
        false,
        null);
  }

  /**
   * Construct an EquiJoin operator. It returns the specified columns from both children when the corresponding columns
   * in compareIndx1 and compareIndx2 match.
   *
   * @param outputColumns the names of the columns in the output schema. If null, the corresponding columns will be
   *          copied from the children.
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @param answerColumns1 the columns of the left child to be returned. Order matters.
   * @param answerColumns2 the columns of the right child to be returned. Order matters.
   * @param setSemanticsLeft if the hash table of the left child should use set semantics.
   * @param setSemanticsRight if the hash table of the right child should use set semantics.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputColumns</tt>, or if
   *        <tt>outputColumns</tt> does not have the correct number of columns and column types.
   */
  public SymmetricHashJoin(
      final Operator left,
      final Operator right,
      final int[] compareIndx1,
      final int[] compareIndx2,
      final int[] answerColumns1,
      final int[] answerColumns2,
      final boolean setSemanticsLeft,
      final boolean setSemanticsRight,
      final List<String> outputColumns) {
    super(left, right);
    Preconditions.checkArgument(compareIndx1.length == compareIndx2.length);
    if (outputColumns != null) {
      Preconditions.checkArgument(
          outputColumns.size() == answerColumns1.length + answerColumns2.length,
          "length mismatch between output column names and columns selected for output");
      Preconditions.checkArgument(
          ImmutableSet.copyOf(outputColumns).size() == outputColumns.size(),
          "duplicate column names in outputColumns");
      this.outputColumns = ImmutableList.copyOf(outputColumns);
    } else {
      this.outputColumns = null;
    }
    leftCompareIndx = MyriaArrayUtils.warnIfNotSet(compareIndx1);
    rightCompareIndx = MyriaArrayUtils.warnIfNotSet(compareIndx2);
    leftAnswerColumns = MyriaArrayUtils.warnIfNotSet(answerColumns1);
    rightAnswerColumns = MyriaArrayUtils.warnIfNotSet(answerColumns2);
    this.setSemanticsLeft = setSemanticsLeft;
    this.setSemanticsRight = setSemanticsRight;
  }

  @Override
  protected Schema generateSchema() {
    Schema leftSchema = getLeft().getSchema();
    if (leftHashTable != null) {
      leftSchema = leftHashTable.getSchema();
    }
    if (leftSchema == null) {
      return null;
    }
    Schema rightSchema = getRight().getSchema();
    if (rightHashTable != null) {
      rightSchema = rightHashTable.getSchema();
    }
    if (rightSchema == null) {
      return null;
    }
    ImmutableList.Builder<Type> types = ImmutableList.builder();
    ImmutableList.Builder<String> names = ImmutableList.builder();

    /* Assert that the compare index types are the same. */
    for (int i = 0; i < rightCompareIndx.length; ++i) {
      int leftIndex = leftCompareIndx[i];
      int rightIndex = rightCompareIndx[i];
      Type leftType = leftSchema.getColumnType(leftIndex);
      Type rightType = rightSchema.getColumnType(rightIndex);
      Preconditions.checkState(
          leftType == rightType,
          "column types do not match for join at index %s: left column type %s [%s] != right column type %s [%s] %s",
          i,
          leftIndex,
          leftType,
          rightIndex,
          rightType,
          getOpId() + "");
    }

    for (int i : leftAnswerColumns) {
      types.add(leftSchema.getColumnType(i));
      names.add(leftSchema.getColumnName(i));
    }

    for (int i : rightAnswerColumns) {
      types.add(rightSchema.getColumnType(i));
      names.add(rightSchema.getColumnName(i));
    }
    if (outputColumns != null) {
      return new Schema(types.build(), outputColumns);
    } else {
      return new Schema(types, names);
    }
  }

  /**
   * @param cntTB current TB
   * @param row current row
   * @param hashTable the buffer holding the tuples to join against
   * @param index the index of hashTable, which the cntTuple is to join with
   * @param fromLeft if the tuple is from child 1
   */
  protected void addToAns(
      final TupleBatch cntTB,
      final int row,
      final MutableTupleBuffer hashTable,
      final int index,
      final boolean fromLeft) {
    List<? extends Column<?>> tbColumns = cntTB.getDataColumns();
    ReadableColumn[] hashTblColumns = hashTable.getColumns(index);
    int tupleIdx = hashTable.getTupleIndexInContainingTB(index);
    if (fromLeft) {
      for (int i = 0; i < leftAnswerColumns.length; ++i) {
        ans.put(i, tbColumns.get(leftAnswerColumns[i]), row);
      }

      for (int i = 0; i < rightAnswerColumns.length; ++i) {
        ans.put(i + leftAnswerColumns.length, hashTblColumns[rightAnswerColumns[i]], tupleIdx);
      }
    } else {
      for (int i = 0; i < leftAnswerColumns.length; ++i) {
        ans.put(i, hashTblColumns[leftAnswerColumns[i]], tupleIdx);
      }

      for (int i = 0; i < rightAnswerColumns.length; ++i) {
        ans.put(i + leftAnswerColumns.length, tbColumns.get(rightAnswerColumns[i]), row);
      }
    }
  }

  @Override
  protected void cleanup() throws DbException {
    // leftHashTable.cleanup();
    // rightHashTable.cleanup();
    ans = null;
  }

  /**
   * In blocking mode, asynchronous EOI semantic may make system hang. Only synchronous EOI semantic works.
   *
   * @return result TB.
   * @throws DbException if any error occurs.
   */
  private TupleBatch fetchNextReadySynchronousEOI() throws DbException {
    final Operator left = getLeft();
    final Operator right = getRight();
    TupleBatch nexttb = ans.popFilled();
    while (nexttb == null) {
      boolean hasnewtuple = false;
      if (!left.eos() && !childrenEOI[0]) {
        TupleBatch tb = left.nextReady();
        if (tb != null) {
          hasnewtuple = true;
          processChildTB(tb, true);
        } else if (left.eoi()) {
          left.setEOI(false);
          childrenEOI[0] = true;
        }
      }
      if (!right.eos() && !childrenEOI[1]) {
        TupleBatch tb = right.nextReady();
        if (tb != null) {
          hasnewtuple = true;
          processChildTB(tb, false);
        } else if (right.eoi()) {
          right.setEOI(false);
          childrenEOI[1] = true;
        }
      }
      nexttb = ans.popFilled();
      if (nexttb != null) {
        return nexttb;
      }
      if (!hasnewtuple) {
        break;
      }
    }
    if (nexttb == null) {
      nexttb = ans.popAny();
    }
    return nexttb;
  }

  @Override
  public void checkEOSAndEOI() {
    final Operator left = getLeft();
    final Operator right = getRight();

    if (left.eos() && right.eos() && ans.numTuples() == 0) {
      setEOS();
      return;
    }

    // EOS could be used as an EOI
    if ((childrenEOI[0] || left.eos()) && (childrenEOI[1] || right.eos()) && ans.numTuples() == 0) {
      setEOI(true);
      Arrays.fill(childrenEOI, false);
    }
  }

  /**
   * Recording the EOI status of the children.
   */
  private final boolean[] childrenEOI = new boolean[2];

  /**
   * consume EOI from Child 1. reset the child's EOI to false 2. record the EOI in childrenEOI[]
   *
   * @param fromLeft true if consuming eoi from left child, false if consuming eoi from right child
   */
  private void consumeChildEOI(final boolean fromLeft) {
    final Operator left = getLeft();
    final Operator right = getRight();
    if (fromLeft) {
      Preconditions.checkArgument(left.eoi());
      left.setEOI(false);
      childrenEOI[0] = true;
    } else {
      Preconditions.checkArgument(right.eoi());
      right.setEOI(false);
      childrenEOI[1] = true;
    }
  }

  /**
   * Note: If this operator is ready for EOS, this function will return true since EOS is a special EOI.
   *
   * @return whether this operator is ready to set itself EOI
   */
  private boolean isEOIReady() {
    if ((childrenEOI[0] || getLeft().eos()) && (childrenEOI[1] || getRight().eos())) {
      return true;
    }
    return false;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    if (!nonBlocking) {
      return fetchNextReadySynchronousEOI();
    }

    if (order.equals(JoinPullOrder.LEFT) || order.equals(JoinPullOrder.LEFT_EOS)) {
      pollLeft = true;
    } else if (order.equals(JoinPullOrder.RIGHT) || order.equals(JoinPullOrder.RIGHT_EOS)) {
      pollLeft = false;
    }

    /* If any full tuple batches are ready, output them. */
    TupleBatch nexttb = ans.popFilled();
    if (nexttb != null) {
      return nexttb;
    }

    /*
     * if both children are eos or both children have recorded eoi, pop any tuples in buffer. If the buffer is empty,
     * set EOS or EOI.
     */
    if (isEOIReady()) {
      nexttb = ans.popAny();
      if (nexttb == null) {
        checkEOSAndEOI();
      }
      return nexttb;
    }

    final Operator left = getLeft();
    final Operator right = getRight();
    int noDataStreak = 0;
    while (noDataStreak < 2 && (!left.eos() || !right.eos())) {

      Operator current;
      if (pollLeft) {
        current = left;
      } else {
        current = right;
      }

      /* process tuple from child */
      TupleBatch tb = current.nextReady();
      if (tb != null) {
        processChildTB(tb, pollLeft);
        noDataStreak = 0;
        /* ALTER: switch to the other child */
        if (order.equals(JoinPullOrder.ALTER)) {
          pollLeft = !pollLeft;
        }
        nexttb = ans.popAnyUsingTimeout();
        if (nexttb != null) {
          return nexttb;
        }
      } else if (current.eoi()) {
        /* if current operator is eoi, consume it, check whether it will cause EOI of this operator */
        consumeChildEOI(pollLeft);
        noDataStreak = 0;
        if (order.equals(JoinPullOrder.ALTER)) {
          pollLeft = !pollLeft;
        }
        /* If this operator is ready to emit EOI (reminder that it needs to clear buffer), break to EOI handle part */
        if (isEOIReady()) {
          break;
        }
      } else {
        if ((pollLeft && order.equals(JoinPullOrder.LEFT_EOS))
            || (!pollLeft && order.equals(JoinPullOrder.RIGHT_EOS))) {
          if (!current.eos()) {
            break;
          }
        }
        /* current.eos() or no data, switch to the other child */
        pollLeft = !pollLeft;
        noDataStreak++;
      }
    }

    /*
     * If the operator is ready to emit EOI, empty its output buffer first. If the buffer is already empty, set EOI
     * and/or EOS
     */
    if (isEOIReady()) {
      nexttb = ans.popAny();
      if (nexttb == null) {
        checkEOSAndEOI();
      }
    }
    return nexttb;
  }

  private List<Integer> getUsedCols(
      final Schema schema, final int[] compareIndx, final int[] answerColumns) {
    List<Integer> usedCols = new ArrayList<Integer>();
    for (int i : compareIndx) {
      usedCols.add(i);
    }
    for (int i : answerColumns) {
      if (!usedCols.contains(i)) {
        usedCols.add(i);
      }
    }
    return usedCols;
  }

  private TupleHashTable createSlicedHashTable(
      final Schema schema, final int[] compareIndx, final int[] answerColumns) {
    int[] usedCols =
        ArrayUtils.toPrimitive(
            getUsedCols(schema, compareIndx, answerColumns).toArray(new Integer[] {}));
    return new TupleHashTable(
        schema.getSubSchema(usedCols), MyriaArrayUtils.range(0, compareIndx.length));
  }

  private Map<Integer, Integer> getInHashTableColumns(
      final Schema schema, final int[] compareIndx, final int[] answerColumns) {
    List<Integer> usedCols = getUsedCols(schema, compareIndx, answerColumns);
    Map<Integer, Integer> cols = new HashMap<Integer, Integer>();
    for (int c : compareIndx) {
      cols.put(usedCols.indexOf(c), c);
    }
    for (int c : answerColumns) {
      cols.put(usedCols.indexOf(c), c);
    }
    return cols;
  }

  Map<Integer, Integer> leftInHashTableColumns, rightInHashTableColumns;

  private int[] getNewIndexMapping(final int[] oldIndex, final Map<Integer, Integer> mapping) {
    int[] ret = new int[oldIndex.length];
    for (int i = 0; i < oldIndex.length; ++i) {
      for (int k : mapping.keySet()) {
        if (mapping.get(k) == oldIndex[i]) {
          ret[i] = k;
          break;
        }
      }
    }
    return ret;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    leftHashTable =
        createSlicedHashTable(getLeft().getSchema(), leftCompareIndx, leftAnswerColumns);
    rightHashTable =
        createSlicedHashTable(getRight().getSchema(), rightCompareIndx, rightAnswerColumns);
    leftInHashTableColumns =
        getInHashTableColumns(getLeft().getSchema(), leftCompareIndx, leftAnswerColumns);
    rightInHashTableColumns =
        getInHashTableColumns(getRight().getSchema(), rightCompareIndx, rightAnswerColumns);
    leftCompareIndx = getNewIndexMapping(leftCompareIndx, leftInHashTableColumns);
    rightCompareIndx = getNewIndexMapping(rightCompareIndx, rightInHashTableColumns);
    leftAnswerColumns = getNewIndexMapping(leftAnswerColumns, leftInHashTableColumns);
    rightAnswerColumns = getNewIndexMapping(rightAnswerColumns, rightInHashTableColumns);
    // leftHashTable = new TupleHashTable(getLeft().getSchema(), leftCompareIndx);
    // rightHashTable = new TupleHashTable(getRight().getSchema(), rightCompareIndx);
    leftHashTable.triggers = triggers;
    rightHashTable.triggers = triggers;
    leftHashTable.name = "op" + getOpId() + "left";
    rightHashTable.name = "op" + getOpId() + "right";
    ans = new TupleBatchBuffer(getSchema());
    nonBlocking =
        (QueryExecutionMode) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE)
            == QueryExecutionMode.NON_BLOCKING;
    if (triggers != null) {
      for (int t : triggers) {
        if (t == 0) {
          System.out.println("sysgcopstats\t" + leftHashTable.dumpStats());
          System.gc();
        }
      }
    }
  }

  /**
   * The query execution mode is nonBlocking.
   */
  private transient boolean nonBlocking = true;

  /**
   * @param tb the incoming TupleBatch for processing join.
   * @param fromLeft if the tb is from left.
   */
  protected void processChildTB(TupleBatch tb, final boolean fromLeft) {
    List<Column<?>> columns = new ArrayList<Column<?>>();
    if (fromLeft) {
      for (int i : leftInHashTableColumns.keySet()) {
        columns.add(tb.getDataColumns().get(leftInHashTableColumns.get(i)));
      }
      tb = new TupleBatch(leftHashTable.getSchema(), columns);
    } else {
      for (int i : rightInHashTableColumns.keySet()) {
        columns.add(tb.getDataColumns().get(rightInHashTableColumns.get(i)));
      }
      tb = new TupleBatch(rightHashTable.getSchema(), columns);
    }

    final Operator left = getLeft();
    final Operator right = getRight();
    /* delete one child's hash table if the other reaches EOS. */
    if (left.eos()) {
      // rightHashTable = null;
    }
    if (right.eos()) {
      // leftHashTable = null;
    }

    final boolean useSetSemantics = fromLeft && setSemanticsLeft || !fromLeft && setSemanticsRight;
    TupleHashTable buildHashTable = null;
    TupleHashTable probeHashTable = null;
    int[] buildCompareColumns = null;
    if (fromLeft) {
      buildHashTable = leftHashTable;
      probeHashTable = rightHashTable;
      buildCompareColumns = leftCompareIndx;
    } else {
      buildHashTable = rightHashTable;
      probeHashTable = leftHashTable;
      buildCompareColumns = rightCompareIndx;
    }
    for (int row = 0; row < tb.numTuples(); ++row) {
      IntIterator iter = probeHashTable.getIndices(tb, buildCompareColumns, row).intIterator();
      while (iter.hasNext()) {
        addToAns(tb, row, probeHashTable.getData(), iter.next(), fromLeft);
      }
      addToHashTable(tb, buildCompareColumns, row, buildHashTable, useSetSemantics);
    }
  }

  /**
   * @param tb the source TupleBatch
   * @param row the row number to get added to hash table
   * @param hashTable the target hash table
   * @param hashTable1IndicesLocal hash table 1 indices local
   * @param hashCode the hashCode of the tb.
   * @param replace if need to replace the hash table with new values.
   */
  private void addToHashTable(
      final TupleBatch tb,
      final int[] compareIndx,
      final int row,
      final TupleHashTable hashTable,
      final boolean replace) {
    if (replace) {
      if (hashTable.replace(tb, compareIndx, row)) {
        return;
      }
    }
    hashTable.addTuple(tb, compareIndx, row, false);
  }

  /**
   * @return the total number of tuples in hash tables
   */
  public long getNumTuplesInHashTables() {
    long sum = 0;
    if (leftHashTable != null) {
      sum += leftHashTable.numTuples();
    }
    if (rightHashTable != null) {
      sum += rightHashTable.numTuples();
    }
    return sum;
  }

  /** Join pull order options. */
  public enum JoinPullOrder {
    /** Alternatively. */
    ALTER,
    /** Pull from the left child whenever there is data available. */
    LEFT,
    /** Pull from the right child whenever there is data available. */
    RIGHT,
    /** Pull from the left child until it reaches EOS. */
    LEFT_EOS,
    /** Pull from the right child until it reaches EOS. */
    RIGHT_EOS
  }

  /**
   * Set the pull order.
   *
   * @param order the pull order.
   */
  public void setPullOrder(final JoinPullOrder order) {
    this.order = order;
  }

  @Override
  public String dumpOpStats() {
    if (leftHashTable == null || rightHashTable == null) {
      return "";
    }
    return "\t" + leftHashTable.dumpStats() + "\t" + rightHashTable.dumpStats();
  }
}
