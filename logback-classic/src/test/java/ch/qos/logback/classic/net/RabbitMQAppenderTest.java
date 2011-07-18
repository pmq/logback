/**
 * Logback: the reliable, generic, fast and flexible logging framework.
 * Copyright (C) 1999-2011, QOS.ch. All rights reserved.
 *
 * This program and the accompanying materials are dual-licensed under
 * either the terms of the Eclipse Public License v1.0 as published by
 * the Eclipse Foundation
 *
 *   or (per the licensee's choosing)
 *
 * under the terms of the GNU Lesser General Public License version 2.1
 * as published by the Free Software Foundation.
 */
package ch.qos.logback.classic.net;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.json.JsonLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.ContextBase;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

/**
 * @author Pierre Queinnec
 */
public class RabbitMQAppenderTest extends TestCase {

  private final static String EXCHANGE_PREFIX = "logback-";
  private final static String ROUTING_KEY = "logback-key";

  private Context context;
  private RabbitMQAppender appender;

  private Connection connection;
  private Channel channel;
  private String queue;

  @Override
  protected void setUp() throws Exception {
    long start = System.currentTimeMillis();
    String exchangeName = EXCHANGE_PREFIX + start;

    context = new ContextBase();
    appender = new RabbitMQAppender();
    appender.setContext(context);
    appender.setName("rabbitmq");

    appender.setExchangeName(exchangeName);
    appender.setRoutingKey(ROUTING_KEY);

    ConnectionFactory connectionFactory = new ConnectionFactory();
    this.connection = connectionFactory.newConnection();
    this.channel = this.connection.createChannel();

    this.channel.exchangeDeclare(exchangeName, "direct");
    System.out.println("using exchange: " + exchangeName);

    DeclareOk queueDeclareOk = this.channel.queueDeclare();
    this.queue = queueDeclareOk.getQueue();
    System.out.println("using queue: " + this.queue);

    this.channel.queueBind(this.queue, exchangeName, ROUTING_KEY);

    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    this.appender = null;
    this.context = null;

    if (this.channel != null) {
      this.channel.close();
      this.channel = null;
    }

    if (this.connection != null) {
      this.connection.close();
      this.connection = null;
    }

    super.tearDown();
  }

  public void testAppendOk() {
    appender.start();

    ILoggingEvent le = createLoggingEvent();
    appender.append(le);
    System.out.println("sent log");

    try {
      this.consume(channel, queue, 1);

    } catch (IOException ioe) {
      fail(ioe.getMessage());
    }
  }

  public void testWireFormatPlainText() {
    appender.setWireFormat(RabbitMQAppender.MODE_PLAIN_TEXT);
    appender.setPattern("%d{HH:mm:ss.SSS} %-5level - %msg%n");
    // appender.setIncludeCallerData(true);
    appender.start();

    ILoggingEvent le = createLoggingEvent();
    for (int i = 1; i <= 3; i++) {
      appender.append(le);
      assertTrue(appender.isStarted());
    }

    try {
      List<Delivery> deliveries = this.consume(channel, queue, 3);

      for (Delivery currDelivery : deliveries) {
        byte[] currPayload = currDelivery.getBody();
        String currPayloadStr = new String(currPayload);

        System.out.println("Plain text payload was: " + currPayloadStr);
        assertNotNull(currPayloadStr);
      }

    } catch (IOException ioe) {
      fail(ioe.getMessage());
    }
  }

  public void testWireFormatJson() {
    appender.setWireFormat(RabbitMQAppender.MODE_PLAIN_TEXT);
    appender.setLayout(new JsonLayout());
    appender.start();

    ILoggingEvent le = createLoggingEvent();
    for (int i = 1; i <= 3; i++) {
      appender.append(le);
      assertTrue(appender.isStarted());
    }

    try {
      List<Delivery> deliveries = this.consume(channel, queue, 3);

      for (Delivery currDelivery : deliveries) {
        byte[] currPayload = currDelivery.getBody();
        String currPayloadStr = new String(currPayload);

        System.out.println("JSON payload was: " + currPayloadStr);
        assertNotNull(currPayloadStr);
        assertTrue(currPayloadStr.startsWith("{"));
        assertTrue(currPayloadStr.endsWith("}"));
      }

    } catch (IOException ioe) {
      fail(ioe.getMessage());
    }
  }

  public void testWireFormatSerialization() {
    appender.setWireFormat(RabbitMQAppender.MODE_SERIALIZATION);
    appender.start();

    ILoggingEvent le = createLoggingEvent();
    for (int i = 1; i <= 3; i++) {
      appender.append(le);
      assertTrue(appender.isStarted());
    }

    try {
      List<Delivery> deliveries = this.consume(channel, queue, 3);

      for (Delivery currDelivery : deliveries) {
        byte[] currPayload = currDelivery.getBody();

        ByteArrayInputStream bais = new ByteArrayInputStream(currPayload);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object currPayloadObj = ois.readObject();

        assertNotNull(currPayloadObj);
        assertTrue(currPayloadObj instanceof ILoggingEvent);

        System.out.println("Java serialization payload was: " + currPayloadObj);
      }

    } catch (IOException ioe) {
      fail(ioe.getMessage());

    } catch (ClassNotFoundException cnfe) {
      fail(cnfe.getMessage());
    }
  }

  public void testAppendFailure() {
    appender.start();

    // make sure the append method does not work
    appender.setExchangeName(null);
    appender.setRoutingKey(null);

    ILoggingEvent le = createLoggingEvent();
    for (int i = 1; i <= 3; i++) {
      appender.append(le);
      assertEquals(i, context.getStatusManager().getCount());
      assertTrue(appender.isStarted());
    }

    appender.append(le);
    assertEquals(4, context.getStatusManager().getCount());
    assertFalse(appender.isStarted());
  }

  private List<Delivery> consume(Channel channel, String queue,
      int nbOfEventsToConsume) throws IOException {

    List<Delivery> deliveries = new ArrayList<QueueingConsumer.Delivery>();
    int nbOfEventsRemaining = nbOfEventsToConsume;
    boolean autoAck = false;

    QueueingConsumer consumer = new QueueingConsumer(channel);
    channel.basicConsume(queue, autoAck, consumer);
    while (nbOfEventsRemaining > 0) {
      QueueingConsumer.Delivery delivery;
      try {
        delivery = consumer.nextDelivery();
        deliveries.add(delivery);
        nbOfEventsRemaining--;

      } catch (InterruptedException ie) {
        continue;
      }

      // DEBUG
      System.out.println(delivery);
      System.out.println('\t' + delivery.getProperties().toString());
      System.out.println('\t' + delivery.getEnvelope().getRoutingKey());
      System.out.println("---------------------------");

      channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
    }

    return deliveries;
  }

  private ILoggingEvent createLoggingEvent() {
    LoggingEvent le = new LoggingEvent();
    le.setLevel(Level.DEBUG);
    le.setMessage("test message");
    le.setTimeStamp(System.currentTimeMillis());
    le.setThreadName(Thread.currentThread().getName());
    return le;
  }

}
