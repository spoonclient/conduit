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
package com.inmobi.conduit;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.SourceStream;
import com.inmobi.conduit.local.TestLocalStreamService;

import org.testng.Assert;

import java.util.ArrayList;
import org.testng.annotations.Test;

import org.apache.log4j.Logger;

import java.io.File;
import java.util.Calendar;
import java.util.GregorianCalendar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PublishMissingPathsTest {
  
  private static Logger LOG = Logger.getLogger(PublishMissingPathsTest.class);

  public static void testPublishMissingPaths(AbstractServiceTest service,
      boolean local)
      throws Exception {

    FileSystem fs = FileSystem.getLocal(new Configuration());
    Calendar behinddate = new GregorianCalendar();
    Calendar todaysdate = new GregorianCalendar();

    String basePath = null;
    if (local)
      basePath = service.getCluster().getLocalFinalDestDirRoot();
    else
      basePath = service.getCluster().getFinalDestDirRoot();
    
    behinddate
        .setTimeInMillis(behinddate.getTimeInMillis() - (3600 * 2 * 1000));
    behinddate.set(Calendar.SECOND, 0);
    
    LOG.debug("Difference between times streams_publish: "
        + String.valueOf(todaysdate.getTimeInMillis()
            - behinddate.getTimeInMillis()));
    String basepublishPaths = basePath + "streams_publish" + File.separator;
    String publishPaths = basepublishPaths
        + Cluster.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());
    
    LOG.debug("Create Missing Directory for streams_publish: " + publishPaths);

    fs.mkdirs(new Path(publishPaths));
    
    service.publishMissingPaths();
    
    VerifyMissingPublishPaths(fs, todaysdate.getTimeInMillis(), behinddate,
        basepublishPaths);
    
    /*
     * todaysdate.add(Calendar.HOUR_OF_DAY, 2);
     * 
     * service.publishMissingPaths();
     * 
     * VerifyMissingPublishPaths(fs, todaysdate.getTimeInMillis(), behinddate,
     * basepublishPaths);
     */
    
    fs.delete(new Path(basepublishPaths), true);
  }
  
  public static void VerifyMissingPublishPaths(FileSystem fs, long todaysdate,
      Calendar behinddate, String basepublishPaths) throws Exception {
    long diff = todaysdate - behinddate.getTimeInMillis();
    while (diff > 180000) {
      String checkcommitpath = basepublishPaths + File.separator
          + Cluster.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());
      LOG.debug("Checking for Created Missing Path: " + checkcommitpath);
      Assert.assertTrue(fs.exists(new Path(checkcommitpath)));
      behinddate.add(Calendar.MINUTE, 1);
      diff = todaysdate - behinddate.getTimeInMillis();
    }
  }
  
  @Test
  public void testPublishMissingPaths() throws Exception {
    ConduitConfigParser configParser = new ConduitConfigParser(
        "test-lss-pub-conduit.xml");
    
    ConduitConfig config = configParser.getConfig();
    
    FileSystem fs = FileSystem.getLocal(new Configuration());
    
    ArrayList<Cluster> clusterList = new ArrayList<Cluster>(config
        .getClusters().values());
    Cluster cluster = clusterList.get(0);
    TestLocalStreamService service = new TestLocalStreamService(config,
        cluster, new FSCheckpointProvider(cluster.getCheckpointDir()));
    
    ArrayList<SourceStream> sstreamList = new ArrayList<SourceStream>(config
        .getSourceStreams().values());
    
    SourceStream sstream = sstreamList.get(0);
    
    Calendar behinddate = new GregorianCalendar();

    behinddate.add(Calendar.HOUR_OF_DAY, -2);
    behinddate.set(Calendar.SECOND, 0);
    
    String basepublishPaths = cluster.getLocalFinalDestDirRoot()
        + sstream.getName() + File.separator;
    String publishPaths = basepublishPaths
        + Cluster.getDateAsYYYYMMDDHHMNPath(behinddate.getTime());
    
    fs.mkdirs(new Path(publishPaths));
    {
      Calendar todaysdate = new GregorianCalendar();
      
      service.publishMissingPaths();
      
      VerifyMissingPublishPaths(fs, todaysdate.getTimeInMillis(), behinddate,
          basepublishPaths);
    }

    {
      Calendar todaysdate = new GregorianCalendar();

      service.publishMissingPaths();
      
      VerifyMissingPublishPaths(fs, todaysdate.getTimeInMillis(), behinddate,
          basepublishPaths);
    }
    
    fs.delete(new Path(cluster.getRootDir()), true);
    
    fs.close();
  }
}
