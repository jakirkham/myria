package edu.washington.escience.myriad.parallel;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.util.IPCUtils;

/**
 * The producer part of the Collect Exchange operator.
 * 
 * The producer actively pushes the tuples generated by the child operator to the paired CollectConsumer.
 * 
 */
public final class CollectProducer extends Producer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param child the child who provides data for this producer to distribute.
   * @param operatorID destination operator the data goes
   * @param collectConsumerWorkerID destination worker the data goes.
   * */
  public CollectProducer(final Operator child, final ExchangePairID operatorID, final int collectConsumerWorkerID) {
    super(child, operatorID, collectConsumerWorkerID);
  }

  @Override
  protected void consumeTuples(final TupleBatch tb) throws DbException {
    TransportMessage dm = null;
    tb.compactInto(getBuffers()[0]);

    while ((dm = getBuffers()[0].popFilledAsTM()) != null) {
      // getChannels()[0].write(dm);
      try {
        writeMessage(getChannels()[0], dm);
      } catch (InterruptedException e) {
        throw new DbException(e);
      }

    }
  }

  @Override
  protected void childEOS() throws DbException {
    TransportMessage dm = null;
    while ((dm = getBuffers()[0].popAnyAsTM()) != null) {
      // getChannels()[0].write(dm);
      try {
        writeMessage(getChannels()[0], dm);
      } catch (InterruptedException e) {
        throw new DbException(e);
      }
    }
    // getChannels()[0].write(IPCUtils.EOS);
    try {
      writeMessage(getChannels()[0], IPCUtils.EOS);
    } catch (InterruptedException e) {
      throw new DbException(e);
    }
  }

  @Override
  protected void childEOI() throws DbException {
    TransportMessage dm = null;
    while ((dm = getBuffers()[0].popAnyAsTM()) != null) {
      // getChannels()[0].write(dm);
      try {
        writeMessage(getChannels()[0], dm);
      } catch (InterruptedException e) {
        throw new DbException(e);
      }
    }
    // getChannels()[0].write(IPCUtils.EOI);
    try {
      writeMessage(getChannels()[0], IPCUtils.EOI);
    } catch (InterruptedException e) {
      throw new DbException(e);
    }
  }

}
