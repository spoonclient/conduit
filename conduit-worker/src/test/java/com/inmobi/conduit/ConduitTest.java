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
import com.inmobi.conduit.Conduit;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.SourceStream;
import com.inmobi.conduit.TestMiniClusterUtil;
import com.inmobi.conduit.distcp.MergeMirrorStreamTest;
import com.inmobi.conduit.distcp.MergedStreamService;
import com.inmobi.conduit.distcp.MirrorStreamService;
import com.inmobi.conduit.distcp.TestMergedStreamService;
import com.inmobi.conduit.distcp.TestMirrorStreamService;
import com.inmobi.conduit.local.LocalStreamService;
import com.inmobi.conduit.local.TestLocalStreamService;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.util.TimerTask;
import java.util.GregorianCalendar;
import java.util.Calendar;
import java.util.Timer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.HashSet;
import java.util.Map;
import org.testng.annotations.Test;

import java.util.Set;

public class ConduitTest extends TestMiniClusterUtil {
  
  private static final Log LOG = LogFactory.getLog(MergeMirrorStreamTest.class);
  
  // @BeforeSuite
  public void setup() throws Exception {
    super.setup(2, 2, 1);
  }
  
  // @AfterSuite
  public void cleanup() throws Exception {
    super.cleanup();
  }

  public static class conduitServiceTest extends Conduit {
    public conduitServiceTest(ConduitConfig config,
        Set<String> clustersToProcess) {
      super(config, clustersToProcess);
    }
  
    @Override
    protected LocalStreamService getLocalStreamService(ConduitConfig config,
        Cluster cluster) {
      return new TestLocalStreamService(config, cluster,
          new FSCheckpointProvider(cluster.getCheckpointDir()));
    }
    
    @Override
    protected MergedStreamService getMergedStreamService(ConduitConfig config,
        Cluster srcCluster, Cluster dstCluster) throws Exception {
      return new TestMergedStreamService(config,
          srcCluster, dstCluster);
    }
    
    @Override
    protected MirrorStreamService getMirrorStreamService(ConduitConfig config,
        Cluster srcCluster, Cluster dstCluster) throws Exception {
      return new TestMirrorStreamService(config,
          srcCluster, dstCluster);
    }
    
  }
  
  private static conduitServiceTest testService = null;

  // @Test
  public void testconduit() throws Exception {
    testconduit("testconduitService_simple.xml");
  }
  
  private void testconduit(String filename) throws Exception {
    ConduitConfigParser configParser = new ConduitConfigParser(filename);
    ConduitConfig config = configParser.getConfig();
    Set<String> clustersToProcess = new HashSet<String>();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    
    for (Map.Entry<String, Cluster> cluster : config.getClusters().entrySet()) {
      String jobTracker = super.CreateJobConf().get("mapred.job.tracker");
      cluster.getValue().getHadoopConf().set("mapred.job.tracker", jobTracker);
    }

    for (Map.Entry<String, SourceStream> sstream : config.getSourceStreams()
        .entrySet()) {
      clustersToProcess.addAll(sstream.getValue().getSourceClusters());
    }
    
    testService = new conduitServiceTest(config, clustersToProcess);
    
    Timer timer = new Timer();
    Calendar calendar = new GregorianCalendar();
    calendar.add(Calendar.MINUTE, 5);
    
    timer.schedule(new TimerTask() { 
      public void run() {
        try {
          LOG.info("Stopping conduit Test Service");
          testService.stop();
          LOG.info("Done stopping conduit Test Service");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }, calendar.getTime());
    
    LOG.info("Starting conduit Test Service");
    testService.startconduit();
    
    for (Map.Entry<String, Cluster> cluster : config.getClusters().entrySet()) {
      fs.delete(new Path(cluster.getValue().getRootDir()), true);
    }
    
    LOG.info("Done with conduit Test Service");
  }

}
