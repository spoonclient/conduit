/*
 * Copyright 2012 InMobi
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.inmobi.conduit.purge;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.local.LocalStreamServiceTest;
import com.inmobi.conduit.local.TestLocalStreamService;
import com.inmobi.conduit.purge.DataPurgerService;
import com.inmobi.conduit.utils.CalendarHelper;

import java.text.NumberFormat;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;



@Test
public class DataPurgerServiceTest {
  private static Logger LOG = Logger.getLogger(DataPurgerServiceTest.class);
  DateFormat dateFormat = new SimpleDateFormat("yyyy:MM:dd:HH:mm");

  public void isPurgeTest1() {
    DataPurgerService service = buildPurgerService();
    Calendar date = new GregorianCalendar();
    date.add(Calendar.HOUR, -1);

    boolean status = service.isPurge(date, new Integer(1));
    LOG.info("isPurgeTest1 streamDate [" + dateFormat.format(date.getTime())+
    "] shouldPurge [" + status + "]" );
    assert  status == true;
  }


  public void isPurgeTest2() {
    DataPurgerService service = buildPurgerService();
    Calendar date = new GregorianCalendar();
    date.add(Calendar.HOUR, -3);
    boolean status = service.isPurge(date, new Integer(1));
    LOG.info("isPurgeTest2 streamDate [" + dateFormat.format(date.getTime())
    + "] shouldPurge [" + status + "]" );
    assert  status == true;
  }


  public void isPurgeTest3() {
    DataPurgerService service = buildPurgerService();
    Calendar date = new GregorianCalendar();
    boolean status = service.isPurge(date, new Integer(24));
    LOG.info("isPurgeTest3 streamDate [" + dateFormat.format(date.getTime())
    + "] shouldPurge [" + status + "]" );
    assert status  == false;
  }

  public void isPurgeTest4() {
    DataPurgerService service = buildPurgerService();
    Calendar date = new GregorianCalendar();
    date.add(Calendar.HOUR, -2);
    boolean status = service.isPurge(date, new Integer(3));
    LOG.info("isPurgeTest4 streamDate [" + dateFormat.format(date.getTime())
    + "] shouldPurge [" + status + "]" );
    assert status  == false;
  }

  public void isPurgeTest5() {
    DataPurgerService service = buildPurgerService();
    Calendar date = new GregorianCalendar();
    date.add(Calendar.HOUR, -3);
    boolean status = service.isPurge(date, new Integer(2));
    LOG.info("isPurgeTest5 streamDate [" + dateFormat.format(date.getTime())
    + "] shouldPurge [" + status + "]" );
    assert status  == true;
  }

  public void isPurgeTest6() {
    DataPurgerService service = buildPurgerService();
    Calendar date = new GregorianCalendar();
    date.add(Calendar.DAY_OF_MONTH, -3);
    boolean status = service.isPurge(date, new Integer(24 * 3));
    LOG.info("isPurgeTest6 streamDate [" + dateFormat.format(date.getTime())
    + "] shouldPurge [" + status + "]" );
    assert status  == true;
  }

  public void isPurgeTest7() {
    DataPurgerService service = buildPurgerService();
    Calendar date = new GregorianCalendar();
    date.add(Calendar.DAY_OF_MONTH, -2);
    boolean status = service.isPurge(date, new Integer(96));
    LOG.info("isPurgeTest6 streamDate [" + dateFormat.format(date.getTime())
        + "] shouldPurge [" + status + "]");
    assert status == false;
  }

  public void isPurgeTest8() {
    DataPurgerService service = buildPurgerService();
    Calendar date = new GregorianCalendar();
    date.add(Calendar.HOUR, -1);
    boolean status = service.isPurge(date, new Integer(10));
    LOG.info("isPurgeTest6 streamDate [" + dateFormat.format(date.getTime())
        + "] shouldPurge [" + status + "]");
    assert status == false;
  }

  private class TestDataPurgerService extends DataPurgerService {
    public TestDataPurgerService(ConduitConfig config, Cluster cluster)
        throws Exception {
      super(config, cluster);
    }

    public void runOnce() throws Exception {
      super.execute();
    }

    public Integer getRetentionTimes(String streamName) {
      return super.getRetentionPeriod(streamName);
    }

  }

  public void testDefaultRetentionTimes() throws Exception {

    LOG.info("Parsing XML test-retention-conduit.xml");
    ConduitConfigParser configparser = new ConduitConfigParser(
        "test-retention-conduit.xml");
    ConduitConfig config = configparser.getConfig();

    for (Cluster cluster : config.getClusters().values()) {

      LOG.info("Creating Service for Cluster " + cluster.getName());
      TestDataPurgerService service = new TestDataPurgerService(config, cluster);
      
      service.runOnce();

      LOG.info("Getting Retention Period for test1");
      Integer Retention = service.getRetentionTimes("test1");

      LOG.info("Retention Period for " + cluster.getName()
          + " test1 stream is " + Retention);

      if (cluster.getName().compareTo("testcluster1") == 0) {
        LOG.info("Testing for testcluster1 " + Retention.intValue());
        Assert.assertEquals(Retention.intValue(), 48);
      }

      if (cluster.getName().compareTo("testcluster2") == 0) {
        LOG.info("Testing for testcluster2 " + Retention.intValue());
        Assert.assertEquals(Retention.intValue(), 46);
      }

      if (cluster.getName().compareTo("testcluster3") == 0) {
        LOG.info("Testing for testcluster2 " + Retention.intValue());
        Assert.assertEquals(Retention.intValue(), 50);
      }

      Retention = service.getRetentionTimes("dummydummyname");

      LOG.info("Testing for dummydummyname " + Retention.intValue());
      Assert.assertEquals(Retention.intValue(), 48);
    }
  }

  final static int NUM_OF_FILES = 35;
  
  private static final NumberFormat idFormat = NumberFormat.getInstance();
  
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(5);
  }

  private void createTestPurgefiles(FileSystem fs, Cluster cluster,
      Calendar date)
      throws Exception {
    for(String streamname: cluster.getSourceStreams()) {
      String[] files = new String[NUM_OF_FILES];
      String datapath = Cluster
          .getDateAsYYYYMMDDHHMNPath(date.getTime());
      String commitpath = cluster.getLocalFinalDestDirRoot() + File.separator
          + streamname + File.separator + datapath;
      String mergecommitpath = cluster.getFinalDestDirRoot() + File.separator
          + streamname + File.separator + datapath;
      String trashpath = cluster.getTrashPath() + File.separator
          + CalendarHelper.getDateAsString(date) + File.separator;
      fs.mkdirs(new Path(commitpath));

      for (int j = 0; j < NUM_OF_FILES; ++j) {
        files[j] = new String(cluster.getName() + "-"
            + TestLocalStreamService.getDateAsYYYYMMDDHHmm(new Date()) + "_"
            + idFormat.format(j));
        {
          Path path = new Path(commitpath + File.separator + files[j]);
          // LOG.info("Creating streams_local File " + path.getName());
          FSDataOutputStream streamout = fs.create(path);
          streamout.writeBytes("Creating Test data for teststream "
              + path.toString());
          streamout.close();
          Assert.assertTrue(fs.exists(path));
        }
        {
          Path path = new Path(mergecommitpath + File.separator + files[j]);
          // LOG.info("Creating streams File " + path.getName());
          FSDataOutputStream streamout = fs.create(path);
          streamout.writeBytes("Creating Test data for teststream "
              + path.toString());
          streamout.close();
          Assert.assertTrue(fs.exists(path));
        }

        {
          Path path = new Path(trashpath + File.separator
              + String.valueOf(date.get(Calendar.HOUR_OF_DAY)) + File.separator
              + files[j]);
          // LOG.info("Creating trash File " + path.toString());
          FSDataOutputStream streamout = fs.create(path);
          streamout.writeBytes("Creating Test trash data for teststream "
              + path.getName());
          streamout.close();
          Assert.assertTrue(fs.exists(path));
        }
      }
    }

  }

  private void verifyPurgefiles(FileSystem fs, Cluster cluster, Calendar date,
      boolean checkexists, boolean checktrashexists) throws Exception {
    for (String streamname : cluster.getSourceStreams()) {
      String datapath = Cluster
          .getDateAsYYYYMMDDHHMNPath(date.getTime());
      String commitpath = cluster.getLocalFinalDestDirRoot() + File.separator
          + streamname + File.separator + datapath;
      String mergecommitpath = cluster.getFinalDestDirRoot() + File.separator
          + streamname + File.separator + datapath;
      String trashpath = cluster.getTrashPath() + File.separator
          + CalendarHelper.getDateAsString(date) + File.separator;
      {
        Path path = new Path(commitpath);
        LOG.info("Verifying File " + path.toString());
        Assert.assertEquals(fs.exists(path), checkexists);
      }
      {
        Path path = new Path(mergecommitpath);
        LOG.info("Verifying File " + path.toString());
        Assert.assertEquals(fs.exists(path), checkexists);
      }

      {
        Path path = new Path(trashpath + File.separator
            + String.valueOf(date.get(Calendar.HOUR_OF_DAY)));
        LOG.info("Verifying File " + path.toString());
        Assert.assertEquals(fs.exists(path), checktrashexists);
      }
    }
  }

  private void testPurgerService(String testfilename, int numofhourstoadd,
      boolean checkifexists,
      boolean checktrashexists)
      throws Exception {
    ConduitConfigParser configparser = new ConduitConfigParser(testfilename);
    ConduitConfig config = configparser.getConfig();
    
    for (Cluster cluster : config.getClusters().values()) {
      TestDataPurgerService service = new TestDataPurgerService(
          config, cluster);
      
      FileSystem fs = FileSystem.getLocal(new Configuration());
      fs.delete(new Path(cluster.getRootDir()), true);
      
      Calendar todaysdate = new GregorianCalendar(Calendar.getInstance()
          .getTimeZone());
      todaysdate.add(Calendar.HOUR, numofhourstoadd);
  
      createTestPurgefiles(fs, cluster, todaysdate);
  
      service.runOnce();
  
      verifyPurgefiles(fs, cluster, todaysdate, checkifexists, checktrashexists);
      fs.delete(new Path(cluster.getRootDir()), true);
      fs.close();
    }
  }

  public void testPurgerService() throws Exception {

    LOG.info("Working for file test-dps-conduit_X_1.xml");
    testPurgerService("test-dps-conduit_X_1.xml", -2, false, false);
    testPurgerService("test-dps-conduit_X_1.xml", -1, true, false);
    LOG.info("Working for file test-dps-conduit_X_4.xml");
    testPurgerService("test-dps-conduit_X_4.xml", -3, false, true);
    testPurgerService("test-dps-conduit_X_4.xml", -1, true, true);
  }
  private DataPurgerService buildPurgerService() {
    DataPurgerService service;
    try {
      ConduitConfig config = LocalStreamServiceTest.buildTestconduitConfig("local", "file:///tmp",
 "datapurger", "48", "24");
      service = new TestDataPurgerService(config, config.getClusters().get(
          "cluster1"));
    }
    catch (Exception e) {
      LOG.error("Error in creating DataPurgerService", e);
      return null;
    }
    return service;
  }
}
