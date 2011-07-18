package ch.qos.logback.classic.net;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.net.RabbitMQAppenderBase;

/**
 * A simple appender that publishes events to a RabbitMQ server. The events are
 * either serialized or converted to plain text, then transmitted as an AMQP message.
 * <p>
 * For more information about this appender, please refer to:
 * http://logback.qos.ch/manual/appenders.html#RabbitMQAppender
 * 
 * @author Pierre Queinnec
 */
public class RabbitMQAppender extends RabbitMQAppenderBase<ILoggingEvent> {

  // FIXME copied from the JMS appenders but should be overridable
  private final static int SUCCESSIVE_FAILURE_LIMIT = 3;

  private final static String DEFAULT_MESSAGE_PATTERN = "%d{HH:mm:ss.SSS} %-5level - %msg";

  public RabbitMQAppender() {
    this.successiveFailureCount = 0;
    this.includeCallerData = false;
    this.pst = new LoggingEventPreSerializationTransformer();
  }

  /**
   * This method called by {@link AppenderBase#doAppend} method to do most
   * of the real appending work.
   */
  @Override
  protected void append(ILoggingEvent event) {
    if (!this.isStarted()) {
      return;
    }

    ByteArrayOutputStream baos = null;
    ObjectOutputStream oos = null;

    try {
      if (this.includeCallerData) {
        event.getCallerData();
      }

      byte[] messageBodyBytes = null;

      switch (this.getWireFormat()) {
      case PLAIN_TEXT:
        String messageBody = this.getLayout().doLayout(event);
        messageBodyBytes = messageBody.getBytes();

        break;

      case SERIALIZATION:
        Serializable so = this.pst.transform(event);
        baos = new ByteArrayOutputStream();
        oos = new ObjectOutputStream(baos);
        oos.writeObject(so);
        oos.flush();

        messageBodyBytes = baos.toByteArray();

        break;

      default:
        this.addError("Wire format selected wasn't understood: "
            + this.wireFormat.toString());
        break;
      }

      this.sendMessage(messageBodyBytes);

      // reset the error counter
      this.successiveFailureCount = 0;

    } catch (Exception e) {
      // DEBUG
      // e.printStackTrace();

      this.successiveFailureCount++;
      if (this.successiveFailureCount > SUCCESSIVE_FAILURE_LIMIT) {
        this.stop();
      }

      this.addError("Could not send message in RabbitMQAppender [" + this.name
          + "].", e);

    } finally {
      // FIXME more checks
      try {
        try {
          if (oos != null) {
            oos.close();
          }

        } finally {
          if (baos != null) {
            baos.close();
          }
        }

      } catch (IOException ioe) {
        // do nothing
        ioe.printStackTrace();
      }
    }
  }

  @Override
  public Layout<ILoggingEvent> getLayout() {
    if (this.layout == null) {
      this.setPattern(null);
    }

    return super.getLayout();
  }

  public void setPattern(String pattern) {
    if (pattern == null) {
      pattern = DEFAULT_MESSAGE_PATTERN;
    }

    PatternLayout patternLayout = new PatternLayout();
    patternLayout.setContext(this.getContext());
    patternLayout.setPattern(pattern);
    // we don't want a ThrowableInformationConverter appended
    // to the end of the converter chain
    patternLayout.setPostCompileProcessor(null);

    this.layout = patternLayout;
    this.layout.start();
  }

}
