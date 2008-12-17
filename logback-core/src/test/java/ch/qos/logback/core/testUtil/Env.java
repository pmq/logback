/**
 * Logback: the generic, reliable, fast and flexible logging framework.
 * 
 * Copyright (C) 2000-2008, QOS.ch
 * 
 * This library is free software, you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation.
 */
package ch.qos.logback.core.testUtil;

public class Env {

  static public boolean isWindows() {
    return System.getProperty("os.name").indexOf("Windows") != -1;
  }

  static public boolean isLinux() {
    return System.getProperty("os.name").indexOf("Linux") != -1;
  }

  static public boolean isJDK6OrHigher() {
    String javaVersion = System.getProperty("java.version");
    if (javaVersion == null) {
      return false;
    }
    if (javaVersion.startsWith("1.6") || javaVersion.startsWith("1.7")) {
      return true;
    } else {
      return false;
    }
  }

}