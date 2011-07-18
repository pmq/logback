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

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Pierre Queinnec
 */
public class RabbitMQAppenderTestApp {

  private final static String EXCHANGE_PREFIX = "logback-";
  private final static String ROUTING_KEY = "logback-test";

  public static void main(String[] args) throws Exception {
    long start = System.currentTimeMillis();
    String exchangeName = EXCHANGE_PREFIX + start;

    Logger logger = (Logger) LoggerFactory
        .getLogger(RabbitMQAppenderTestApp.class);
    LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
    lc.reset();

    RabbitMQAppender appender = new RabbitMQAppender();
    appender.setContext(lc);
    appender.setName("rabbitmq");

    appender.setExchangeName(exchangeName);
    appender.setRoutingKey(ROUTING_KEY);

    ConnectionFactory connectionFactory = new ConnectionFactory();
    Connection connection = connectionFactory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(exchangeName, "direct");
    System.out.println("using exchange: " + exchangeName);

    DeclareOk queueDeclareOk = channel.queueDeclare();
    String queue = queueDeclareOk.getQueue();
    System.out.println("using queue: " + queue);

    channel.queueBind(queue, exchangeName, ROUTING_KEY);

    appender.start();
    logger.addAppender(appender);

    // JIT
    for (int i = 0; i < 10000; i++) {
      logger.debug("** Hello world. n=" + i);
    }

    long before = System.nanoTime();
    for (int i = 0; i < 10000; i++) {
      logger.debug("** Hello world. n=" + i);
    }
    long after = System.nanoTime();

    System.out.println("Time per logs for 10'000 logs: " + (after - before)
        / 10000);

    StatusPrinter.print(lc.getStatusManager());
  }
}
