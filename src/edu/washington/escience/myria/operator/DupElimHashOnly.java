package edu.washington.escience.myria.operator;

import java.util.Arrays;
import java.util.BitSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import com.gs.collections.api.iterator.LongIterator;
import com.gs.collections.impl.list.mutable.primitive.LongArrayList;
import com.gs.collections.impl.map.mutable.primitive.LongLongHashMap;
import com.gs.collections.impl.map.mutable.primitive.LongObjectHashMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.HashUtils;

/**
 * A simple implementation of duplicate eliminate. It keeps the references to all the TupleBatches which contain unique
 * tuples.
 * */
public final class DupElimHashOnly extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * We store this value instead of a valid hash code to indicate
   * that a given initial hash code is mapped to multiple final hash codes.
   */
  private static final long COLLIDING_KEY = -1;
  /**
   * We return this value from getIfAbsent() to indicate absence,
   * since 0 and -1 are already legitimate values.
   */
  private static final long ABSENT_VALUE = -2;
  /** Stores 128-bit hash codes as map of first 64-bit word to second 64-bit word. */
  private transient LongLongHashMap initialToFinalhashCodes;
  /** Map of first 64-bit words to colliding second 64-bit words. */
  private transient LongObjectHashMap<LongArrayList> collidingFinalHashCodes;

  /**
   * @param child the child
   * */
  public DupElimHashOnly(final Operator child) {
    super(child);
  }

  @Override
  protected void cleanup() throws DbException {}

  /**
   * Do duplicate elimination for the tb.
   *
   * @param tb the TB.
   * @return a new TB with duplicates removed.
   * */
  protected TupleBatch doDupElim(final TupleBatch tb) {
    final int numTuples = tb.numTuples();
    if (numTuples <= 0) {
      return tb;
    }
    final BitSet toRemove = new BitSet(numTuples);
    for (int i = 0; i < numTuples; ++i) {
      final byte[] hashCodeBytes = HashUtils.hashRowBytes(tb, i);
      Preconditions.checkArgument(
          hashCodeBytes.length == 16,
          "Expected 16 bytes in hash code, found %d",
          hashCodeBytes.length);
      byte[] hashCodeBytes1 = Arrays.copyOfRange(hashCodeBytes, 0, 8);
      byte[] hashCodeBytes2 = Arrays.copyOfRange(hashCodeBytes, 8, hashCodeBytes.length);
      long hashCode1 = Longs.fromByteArray(hashCodeBytes1);
      long hashCode2 = Longs.fromByteArray(hashCodeBytes2);
      long hashCodeValue = initialToFinalhashCodes.getIfAbsent(hashCode1, ABSENT_VALUE);
      if (hashCodeValue == ABSENT_VALUE) {
        initialToFinalhashCodes.put(hashCode1, hashCode2);
      } else if (hashCodeValue == COLLIDING_KEY) {
        LongArrayList collidingHashCodes = collidingFinalHashCodes.get(hashCode1);
        Preconditions.checkNotNull(collidingHashCodes);
        Preconditions.checkState(collidingHashCodes.size() > 1);
        LongIterator iter = collidingHashCodes.longIterator();
        while (iter.hasNext()) {
          long hc = iter.next();
          if (hc == hashCode2) {
            // duplicate found
            toRemove.set(i);
            break;
          }
        }
      } else if (hashCodeValue != hashCode2) {
        LongArrayList collidingHashCodes = LongArrayList.newListWith(hashCodeValue, hashCode2);
        Preconditions.checkState(!collidingFinalHashCodes.containsKey(hashCode1));
        collidingFinalHashCodes.put(hashCode1, collidingHashCodes);
        initialToFinalhashCodes.put(hashCode1, COLLIDING_KEY);
      } else {
        // duplicate found
        toRemove.set(i);
      }
    }
    return tb.filterOut(toRemove);
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {

    TupleBatch tb = null;
    tb = getChild().nextReady();
    while (tb != null) {
      tb = doDupElim(tb);
      if (tb.numTuples() > 0) {
        return tb;
      }
      tb = getChild().nextReady();
    }
    return null;
  }

  @Override
  public Schema generateSchema() {
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    return child.getSchema();
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    initialToFinalhashCodes = new LongLongHashMap();
    collidingFinalHashCodes = new LongObjectHashMap<LongArrayList>();
  }
}
