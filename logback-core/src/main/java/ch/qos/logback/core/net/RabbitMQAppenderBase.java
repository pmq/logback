package ch.qos.logback.core.net;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.spi.PreSerializationTransformer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * A simple appender that publishes events to a RabbitMQ server. The events are
 * either serialized or converted to plain text, then transmitted as an AMQP message.
 * <p>
 * For more information about this appender, please refer to:
 * http://logback.qos.ch/manual/appenders.html#RabbitMQAppender
 * 
 * @author Pierre Queinnec
 */
public abstract class RabbitMQAppenderBase<E> extends AppenderBase<E> {

  // FIXME write the manual
  // FIXME rewrite this as an UnsynchronizedAppenderBase, maybe with
  // ThreadLocal<Channel>

  public final static String MODE_SERIALIZATION = "serialization";
  public final static String MODE_PLAIN_TEXT = "plain-text";

  protected PreSerializationTransformer<E> pst;
  protected Layout<E> layout;
  protected boolean includeCallerData;

  protected int successiveFailureCount;
  protected WireFormat wireFormat;

  protected BasicProperties basicProperties;
  protected Connection connection;
  protected Channel channel;

  protected String username;
  protected String password;
  protected String virtualHost;
  protected String host;
  protected Integer port;

  protected String exchangeName;
  protected String routingKey;
  protected Integer deliveryMode;
  protected Map<String, Object> headers;
  protected String contentEncoding;
  protected String contentType;
  protected Integer priority;
  protected String type;
  protected Date timestamp;
  protected String messageId;
  protected String expiration;
  protected String replyTo;
  protected String correlationId;
  protected String appId;
  protected String userId;
  protected String clusterId;
  protected boolean mandatory;
  protected boolean immediate;

  /**
   * Options are activated and become effective only after calling this method.
   */
  @Override
  public void start() {
    try {
      ConnectionFactory connectionFactory = new ConnectionFactory();

      if (this.username != null)
        connectionFactory.setUsername(this.username);

      if (this.password != null)
        connectionFactory.setPassword(this.password);

      if (this.virtualHost != null)
        connectionFactory.setVirtualHost(this.virtualHost);

      if (this.host != null)
        connectionFactory.setHost(this.host);

      if (this.port != null)
        connectionFactory.setPort(this.port);

      this.connection = connectionFactory.newConnection();
      this.channel = connection.createChannel();

    } catch (IOException ioe) {
      ioe.printStackTrace();
    }

    super.start();
  }

  /**
   * Close this RabbitMQAppender. Closing releases all resources used by the
   * appender. A closed appender cannot be re-opened.
   */
  @Override
  public synchronized void stop() {
    try {
      this.channel.close();
      this.connection.close();

    } catch (IOException ioe) {
      ioe.printStackTrace();
    }

    super.stop();
  }

  public BasicProperties getBasicProperties() {
    if (this.basicProperties == null) {
      this.basicProperties = new AMQP.BasicProperties(this.contentType,
          this.contentEncoding, this.headers, this.deliveryMode, this.priority,
          this.correlationId, this.replyTo, this.expiration, this.messageId,
          this.timestamp, this.type, this.userId, this.appId, this.clusterId);
    }

    return this.basicProperties;
  }

  protected void sendMessage(byte[] messageBodyBytes) throws IOException {
    this.channel.basicPublish(this.exchangeName, this.routingKey,
        this.mandatory, this.immediate, this.getBasicProperties(),
        messageBodyBytes);
  }

  public void setBasicProperties(BasicProperties basicProperties) {
    this.basicProperties = basicProperties;
  }

  public void setSuccessiveFailureCount(int successiveFailureCount) {
    this.successiveFailureCount = successiveFailureCount;
  }

  public void setPst(PreSerializationTransformer<E> pst) {
    this.pst = pst;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  public void setChannel(Channel channel) {
    this.channel = channel;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setVirtualHost(String virtualHost) {
    this.virtualHost = virtualHost;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setPort(Integer port) {
    this.port = port;
  }

  public void setExchangeName(String exchangeName) {
    this.exchangeName = exchangeName;
  }

  public void setRoutingKey(String routingKey) {
    this.routingKey = routingKey;
  }

  public void setDeliveryMode(Integer deliveryMode) {
    this.deliveryMode = deliveryMode;
  }

  public void setHeaders(Map<String, Object> headers) {
    this.headers = headers;
  }

  public void setContentEncoding(String contentEncoding) {
    this.contentEncoding = contentEncoding;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  public void setPriority(Integer priority) {
    this.priority = priority;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setTimestamp(Date timestamp) {
    this.timestamp = timestamp;
  }

  public void setMessageId(String messageId) {
    this.messageId = messageId;
  }

  public void setExpiration(String expiration) {
    this.expiration = expiration;
  }

  public void setReplyTo(String replyTo) {
    this.replyTo = replyTo;
  }

  public void setCorrelationId(String correlationId) {
    this.correlationId = correlationId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public void setClusterId(String clusterId) {
    this.clusterId = clusterId;
  }

  public void setMandatory(boolean mandatory) {
    this.mandatory = mandatory;
  }

  public void setImmediate(boolean immediate) {
    this.immediate = immediate;
  }

  public Layout<E> getLayout() {
    return layout;
  }

  public void setLayout(Layout<E> layout) {
    this.layout = layout;
  }

  public void setIncludeCallerData(boolean includeCallerData) {
    this.includeCallerData = includeCallerData;
  }

  public WireFormat getWireFormat() {
    if (this.wireFormat == null) {
      this.wireFormat = WireFormat.PLAIN_TEXT;
    }

    return this.wireFormat;
  }

  public void setWireFormat(WireFormat wireFormat) {
    this.wireFormat = wireFormat;
  }

  public void setWireFormat(String wireFormatStr) {
    this.wireFormat = WireFormat.fromMode(wireFormatStr);
  }

  protected enum WireFormat {
    SERIALIZATION(MODE_SERIALIZATION), PLAIN_TEXT(MODE_PLAIN_TEXT); // JSON(MODE_JSON)

    private String mode;

    private WireFormat(String mode) {
      this.mode = mode;
    }

    public static WireFormat fromMode(String mode) {
      for (WireFormat currWireFormat : values()) {
        if (mode.equalsIgnoreCase(currWireFormat.mode)) {
          return currWireFormat;
        }
      }

      throw new IllegalArgumentException("Wire Format '" + mode
          + "' doesn't exist.");
    }
  }

}
