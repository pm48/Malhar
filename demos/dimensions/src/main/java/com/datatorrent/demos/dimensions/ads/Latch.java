/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.demos.dimensions.ads;

import static com.datatorrent.common.util.ScheduledThreadPoolExecutor.logger;
import java.util.concurrent.TimeoutException;

public class Latch
   {
   private final Object synchObj = new Object();
   private int count;
   private long startTms = 0L;
   boolean wasSignalled = false;

   public Latch(int noThreads)
   {
   synchronized (synchObj) {
   this.count = noThreads;
   }
   }

   public void await(int difference, long timeout) throws InterruptedException, TimeoutException
   {
   synchronized (synchObj) {
   startTms = System.currentTimeMillis();

   //logger.info("difference is" + difference);
   if (difference >= 0) {
   while (count > difference) {
   if (difference > 0) {
   long elapsedTime = System.currentTimeMillis() - startTms;
   if (elapsedTime > 0) {
   synchObj.wait(timeout - elapsedTime);
   }
   else {
   logger.error("Timeout error");
   throw new TimeoutException();
   }
   //logger.info("elapsedTime is " + elapsedTime);
   }
   if (difference <= 0) {
   synchObj.wait(timeout);
   }
   }
   }
   }
   }

   public void countDown()
   {
   synchronized (synchObj) {
   if (--count <= 0) {
   wasSignalled = true;
   synchObj.notifyAll();
   }
   }
   }

   }