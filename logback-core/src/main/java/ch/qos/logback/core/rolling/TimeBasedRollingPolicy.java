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
package ch.qos.logback.core.rolling;

import java.io.File;
import java.util.Date;
import java.util.concurrent.Future;

import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.rolling.helper.*;

/**
 * <code>TimeBasedRollingPolicy</code> is both easy to configure and quite
 * powerful. It allows the roll over to be made based on time. It is possible to
 * specify that the roll over occur once per day, per week or per month.
 * 
 * <p>For more information, please refer to the online manual at
 * http://logback.qos.ch/manual/appenders.html#TimeBasedRollingPolicy
 * 
 * @author Ceki G&uuml;lc&uuml;
 */
public class TimeBasedRollingPolicy<E> extends RollingPolicyBase implements
    TriggeringPolicy<E> {
  static final String FNP_NOT_SET = "The FileNamePattern option must be set before using TimeBasedRollingPolicy. ";
  static final int NO_DELETE_HISTORY = 0;

  // WCS: without compression suffix
  FileNamePattern fileNamePatternWCS;

  private Compressor compressor;
  private RenameUtil renameUtil = new RenameUtil();
  Future<?> future;

  private int maxHistory = NO_DELETE_HISTORY;
  private ArchiveRemover archiveRemover;

  TimeBasedFileNamingAndTriggeringPolicy<E> timeBasedFileNamingAndTriggeringPolicy;

  public void start() {
    // set the LR for our utility object
    renameUtil.setContext(this.context);

    // find out period from the filename pattern
    if (fileNamePatternStr != null) {
      fileNamePattern = new FileNamePattern(fileNamePatternStr, this.context);
      determineCompressionMode();
    } else {
      addWarn(FNP_NOT_SET);
      addWarn(CoreConstants.SEE_FNP_NOT_SET);
      throw new IllegalStateException(FNP_NOT_SET
          + CoreConstants.SEE_FNP_NOT_SET);
    }

    compressor = new Compressor(compressionMode);
    compressor.setContext(context);

    // wcs : without compression suffix
    fileNamePatternWCS = new FileNamePattern(Compressor.computeFileNameStr_WCS(
            fileNamePatternStr, compressionMode), this.context);

    addInfo("Will use the pattern " + fileNamePatternWCS
        + " for the active file");

     if(compressionMode == CompressionMode.ZIP) {
      String zipEntryFileNamePatternStr = transformFileNamePattern2ZipEntry(fileNamePatternStr);
      zipEntryFileNamePattern = new FileNamePattern(zipEntryFileNamePatternStr, context);
    }

    if (timeBasedFileNamingAndTriggeringPolicy == null) {
      timeBasedFileNamingAndTriggeringPolicy = new DefaultTimeBasedFileNamingAndTriggeringPolicy<E>();
    }
    timeBasedFileNamingAndTriggeringPolicy.setContext(context);
    timeBasedFileNamingAndTriggeringPolicy.setTimeBasedRollingPolicy(this);
    timeBasedFileNamingAndTriggeringPolicy.start();

    // the maxHistory property is given to TimeBasedRollingPolicy instead of to
    // the TimeBasedFileNamingAndTriggeringPolicy. This makes it more convenient
    // for the user at the cost of inconsistency here.
    if (maxHistory != NO_DELETE_HISTORY) {
      archiveRemover = timeBasedFileNamingAndTriggeringPolicy.getArchiveRemover();
      archiveRemover.setMaxHistory(maxHistory);
    }

    super.start();
  }

  private String transformFileNamePattern2ZipEntry(String fileNamePatternStr) {
    String slashified = FileFilterUtil.slashify(fileNamePatternStr);
    return FileFilterUtil.afterLastSlash(slashified);
  }

  public void setTimeBasedFileNamingAndTriggeringPolicy(
      TimeBasedFileNamingAndTriggeringPolicy<E> timeBasedTriggering) {
    this.timeBasedFileNamingAndTriggeringPolicy = timeBasedTriggering;
  }

  public TimeBasedFileNamingAndTriggeringPolicy<E> getTimeBasedFileNamingAndTriggeringPolicy() {
    return timeBasedFileNamingAndTriggeringPolicy;
  }

  public void rollover() throws RolloverFailure {

    // when rollover is called the elapsed period's file has
    // been already closed. This is a working assumption of this method.

    String elapsedPeriodsFileName = timeBasedFileNamingAndTriggeringPolicy
        .getElapsedPeriodsFileName();

    String elpasedPeriodStem = FileFilterUtil.afterLastSlash(elapsedPeriodsFileName);


    if (compressionMode == CompressionMode.NONE) {
      if (getParentsRawFileProperty() != null) {
        renameUtil.rename(getParentsRawFileProperty(), elapsedPeriodsFileName);
      } // else { nothing to do if CompressionMode == NONE and parentsRawFileProperty == null }
    } else {
      if (getParentsRawFileProperty() == null) {
        future = asyncCompress(elapsedPeriodsFileName, elapsedPeriodsFileName, elpasedPeriodStem);
      } else {
        future = renamedRawAndAsyncCompress(elapsedPeriodsFileName, elpasedPeriodStem);
      }
    }

    if (archiveRemover != null) {
      archiveRemover.clean(new Date(timeBasedFileNamingAndTriggeringPolicy.getCurrentTime()));
    }
  }

  Future asyncCompress(String nameOfFile2Compress, String nameOfCompressedFile, String innerEntryName)
      throws RolloverFailure {
    AsynchronousCompressor ac = new AsynchronousCompressor(compressor);
    return ac.compressAsynchronously(nameOfFile2Compress, nameOfCompressedFile, innerEntryName);
  }

  Future renamedRawAndAsyncCompress(String nameOfCompressedFile, String innerEntryName)
      throws RolloverFailure {
    String parentsRawFile = getParentsRawFileProperty();
    String tmpTarget = parentsRawFile + System.nanoTime() + ".tmp";
    renameUtil.rename(parentsRawFile, tmpTarget);
    return asyncCompress(tmpTarget, nameOfCompressedFile, innerEntryName);
  }

  /**
   * 
   * The active log file is determined by the value of the parent's filename
   * option. However, in case the file name is left blank, then, the active log
   * file equals the file name for the current period as computed by the
   * <b>FileNamePattern</b> option.
   * 
   * <p>The RollingPolicy must know whether it is responsible for changing the
   * name of the active file or not. If the active file name is set by the user
   * via the configuration file, then the RollingPolicy must let it like it is.
   * If the user does not specify an active file name, then the RollingPolicy
   * generates one.
   * 
   * <p> To be sure that the file name used by the parent class has been
   * generated by the RollingPolicy and not specified by the user, we keep track
   * of the last generated name object and compare its reference to the parent
   * file name. If they match, then the RollingPolicy knows it's responsible for
   * the change of the file name.
   * 
   */
  public String getActiveFileName() {
    String parentsRawFileProperty = getParentsRawFileProperty();
    if (parentsRawFileProperty != null) {
      return parentsRawFileProperty;
    } else {
      return timeBasedFileNamingAndTriggeringPolicy
          .getCurrentPeriodsFileNameWithoutCompressionSuffix();
    }
  }

  public boolean isTriggeringEvent(File activeFile, final E event) {
    return timeBasedFileNamingAndTriggeringPolicy.isTriggeringEvent(activeFile, event);
  }

  /**
   * Get the number of archive files to keep.
   * 
   * @return number of archive files to keep
   */
  public int getMaxHistory() {
    return maxHistory;
  }

  /**
   * Set the maximum number of archive files to keep.
   * 
   * @param maxHistory
   *                number of archive files to keep
   */
  public void setMaxHistory(int maxHistory) {
    this.maxHistory = maxHistory;
  }

  @Override
  public String toString() {
    return "c.q.l.core.rolling.TimeBasedRollingPolicy";
  }
}
