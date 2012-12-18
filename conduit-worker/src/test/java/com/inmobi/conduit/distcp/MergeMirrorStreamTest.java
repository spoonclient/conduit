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
package com.inmobi.conduit.distcp;

import java.util.HashSet;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.DestinationStream;
import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.SourceStream;
import com.inmobi.conduit.TestMiniClusterUtil;
import com.inmobi.conduit.local.TestLocalStreamService;

import org.testng.annotations.BeforeSuite;
import org.testng.annotations.AfterSuite;

import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;


public class MergeMirrorStreamTest extends TestMiniClusterUtil {

  private static final Log LOG = LogFactory.getLog(MergeMirrorStreamTest.class);

  /*
   * Here is the basic idea, create two clusters of different rootdir paths run
   * the local stream service to create all the files in streams_local directory
   * run the merge stream service and verify all the paths are visible in
   * primary cluster
   */
  /**
   * @throws Exception
   */
  @Test
  public void testMergeMirrorStream() throws Exception {
    testMergeMirrorStream("test-mss-conduit.xml");
    // Test with 2 mirror sites
    testMergeMirrorStream("test-mss-conduit_mirror.xml");
  }
  
  @Test(groups = { "integration" })
  public void testAllComboMergeMirrorStream() throws Exception {
    // Test with 1 merged stream only
    testMergeMirrorStream("test-mergedss-conduit.xml");
    // Test with 1 source and 1 merged stream only
    testMergeMirrorStream("test-mergedss-conduit_2.xml");
  }
  
  @BeforeSuite
  public void setup() throws Exception {
    super.setup(2, 6, 1);
  }
  
  @AfterSuite
  public void cleanup() throws Exception {
    super.cleanup();
  }

  private void testMergeMirrorStream(String filename) throws Exception {
    
    ConduitConfigParser parser = new ConduitConfigParser(filename);
    ConduitConfig config = parser.getConfig();
    
    Set<String> clustersToProcess = new HashSet<String>();
    Set<TestLocalStreamService> localStreamServices = new HashSet<TestLocalStreamService>();
    
    for (SourceStream sStream : config.getSourceStreams().values()) {
      for (String cluster : sStream.getSourceClusters()) {
        clustersToProcess.add(cluster);
      }
    }
    
    for (String clusterName : clustersToProcess) {
      Cluster cluster = config.getClusters().get(clusterName);
      cluster.getHadoopConf().set("mapred.job.tracker",
          super.CreateJobConf().get("mapred.job.tracker"));
      TestLocalStreamService service = new TestLocalStreamService(config,
          cluster, new FSCheckpointProvider(cluster.getCheckpointDir()));
      localStreamServices.add(service);
      service.getFileSystem().delete(
          new Path(service.getCluster().getRootDir()), true);
    }
    
    LOG.info("Running LocalStream Service");

    for (TestLocalStreamService service : localStreamServices) {
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();
    }

    Set<TestMergedStreamService> mergedStreamServices = new HashSet<TestMergedStreamService>();
    Set<TestMirrorStreamService> mirrorStreamServices = new HashSet<TestMirrorStreamService>();

    for (Map.Entry<String, Cluster> cluster : config.getClusters().entrySet()) {
      cluster.getValue().getHadoopConf().set("mapred.job.tracker", "local");
    
      Set<String> mergedStreamRemoteClusters = new HashSet<String>();
      Set<String> mirroredRemoteClusters = new HashSet<String>();
      for (DestinationStream cStream : cluster.getValue().getDestinationStreams().values()) {
        //Start MergedStreamConsumerService instances for this cluster for each cluster
        //from where it has to fetch a partial stream and is hosting a primary stream
        //Start MirroredStreamConsumerService instances for this cluster for each cluster
        //from where it has to mirror mergedStreams
  
        for (String cName : config.getSourceStreams().get(cStream.getName())
        .getSourceClusters()) {
          if (cStream.isPrimary())
            mergedStreamRemoteClusters.add(cName);
        }
        if (!cStream.isPrimary())  {
          Cluster primaryCluster = config.getPrimaryClusterForDestinationStream(cStream.getName());
          if (primaryCluster != null)
            mirroredRemoteClusters.add(primaryCluster.getName());
        }
      }
  
  
      for (String remote : mergedStreamRemoteClusters) {
        mergedStreamServices.add(new TestMergedStreamService(config,
            config.getClusters().get(remote), cluster.getValue()));
      }
      for (String remote : mirroredRemoteClusters) {
        mirrorStreamServices.add(new TestMirrorStreamService(config,
            config.getClusters().get(remote), cluster.getValue()));
      }
    }
    
    LOG.info("Running MergedStream Service");

    for (TestMergedStreamService service : mergedStreamServices) {
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();

    }
    
    LOG.info("Running MirrorStreamService Service");

    for (TestMirrorStreamService service : mirrorStreamServices) {
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();

    }

    LOG.info("Cleaning up leftovers");

    for (TestLocalStreamService service : localStreamServices) {
      service.getFileSystem().delete(
          new Path(service.getCluster().getRootDir()), true);
    }
  }
}
