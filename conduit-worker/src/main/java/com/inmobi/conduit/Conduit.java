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
import com.inmobi.conduit.ConduitConstants;
import com.inmobi.conduit.DestinationStream;
import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.distcp.MergedStreamService;
import com.inmobi.conduit.distcp.MirrorStreamService;
import com.inmobi.conduit.local.LocalStreamService;
import com.inmobi.conduit.purge.DataPurgerService;
import com.inmobi.conduit.utils.SecureLoginUtil;
import com.inmobi.conduit.zookeeper.CuratorLeaderManager;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class Conduit implements Service, ConduitConstants {
  private static Logger LOG = Logger.getLogger(Conduit.class);
  private ConduitConfig config;

  public Set<String> getClustersToProcess() {
    return clustersToProcess;
  }

  private final Set<String> clustersToProcess;
  private final List<AbstractService> services = new ArrayList<AbstractService>();


  public Conduit(ConduitConfig config, Set<String> clustersToProcess) {
    this.config = config;
    this.clustersToProcess = clustersToProcess;
  }

  public ConduitConfig getConfig() {
    return config;
  }

  private void init() throws Exception {
    for (Cluster cluster : config.getClusters().values()) {
      if (!clustersToProcess.contains(cluster.getName())) {
        continue;
      }
      //Start LocalStreamConsumerService for this cluster if it's the source of any stream
      if (cluster.getSourceStreams().size() > 0) {
        services.add(getLocalStreamService(config, cluster));
      }

			Set<String> mergedStreamRemoteClusters = new HashSet<String>();
			Set<String> mirroredRemoteClusters = new HashSet<String>();
      for (DestinationStream cStream : cluster.getDestinationStreams().values()) {
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
        services.add(getMergedStreamService(config,
            config.getClusters().get(remote), cluster));
      }
			for (String remote : mirroredRemoteClusters) {
        services.add(getMirrorStreamService(config,
            config.getClusters().get(remote), cluster));
      }
    }

    //Start a DataPurgerService for this Cluster/Clusters to process
    Iterator<String> it = clustersToProcess.iterator();
    while(it.hasNext()) {
      String  clusterName = it.next();
      Cluster cluster =  config.getClusters().get(clusterName);
      LOG.info("Starting Purger for Cluster [" + clusterName + "]");
      //Start a purger per cluster
      services.add(new DataPurgerService(config, cluster));
    }
  }
  
  protected LocalStreamService getLocalStreamService(ConduitConfig config,
      Cluster cluster) {
    return new LocalStreamService(config, cluster, new FSCheckpointProvider(
        cluster.getCheckpointDir()));
  }
  
  protected MergedStreamService getMergedStreamService(ConduitConfig config,
      Cluster srcCluster, Cluster dstCluster) throws Exception {
    return new MergedStreamService(config, srcCluster, dstCluster);
  }
  
  protected MirrorStreamService getMirrorStreamService(ConduitConfig config,
      Cluster srcCluster, Cluster dstCluster) throws Exception {
    return new MirrorStreamService(config, srcCluster, dstCluster);
  }

  @Override
  public void stop() throws Exception {
    for (AbstractService service : services) {
      LOG.info("Stopping [" + service.getName() + "]");
      service.stop();
    }
    LOG.info("conduit Shutdown complete..");
  }

  @Override
  public void join() throws Exception {
    for (AbstractService service : services) {
      LOG.info("Waiting for [" + service.getName() + "] to finish");
      service.join();
    }
  }

  @Override
  public void start() throws Exception{
    startconduit();
    //If all threads are finished release leadership
    System.exit(0);
  }
  
  public void startconduit() throws Exception {
    try {
      init();
      for (AbstractService service : services) {
        service.start();
      }
    } catch (Exception e) {
      LOG.warn("Error is starting service", e);
    }
    // Block this method to avoid losing leadership of current work
    join();
  }

  private static String getProperty(Properties prop, String property) {
    String propvalue = prop.getProperty(property);
    if (new File(propvalue).exists()) {
      return propvalue;
    } else {
      String filePath = ClassLoader.getSystemResource(propvalue).getPath();
      if (new File(filePath).exists())
        return filePath;
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    try {
      if (args.length != 1 ) {
        LOG.error("Usage: com.inmobi.conduit.conduit <conduit.cfg>");
        throw new RuntimeException("Usage: com.inmobi.conduit.conduit " +
        "<conduit.cfg>");
      }
      String cfgFile = args[0].trim();
      Properties prop = new Properties();
      prop.load(new FileReader(cfgFile));

      String log4jFile = getProperty(prop, LOG4J_FILE);
      if (log4jFile == null) {
        LOG.error("log4j.properties incorrectly defined");
        throw new RuntimeException("Log4j.properties not defined");
      }
      PropertyConfigurator.configureAndWatch(log4jFile);
      LOG.info("Log4j Property File [" + log4jFile + "]");

      String clustersStr = prop.getProperty(CLUSTERS_TO_PROCESS);
      if (clustersStr == null || clustersStr.length() == 0) {
        LOG.error("Please provide " + CLUSTERS_TO_PROCESS + " in [" +
        cfgFile + "]");
        throw new RuntimeException("Insufficent information on cluster name");
      }
      String[] clusters = clustersStr.split(",");
      String conduitConfigFile = getProperty(prop, conduit_XML);
      if (conduitConfigFile == null)  {
        LOG.error("conduit Configuration file doesn't exist..can't proceed");
        throw new RuntimeException("Specified conduit config file doesn't " +
        "exist");
      }
      String zkConnectString = prop.getProperty(ZK_ADDR);
      if (zkConnectString == null || zkConnectString.length() == 0) {
        LOG.error("Zookeper connection string not specified");
        throw new RuntimeException("Zoookeeper connection string not " +
        "specified");
      }
      String principal = prop.getProperty(KRB_PRINCIPAL);
      String keytab = getProperty(prop, KEY_TAB_FILE);
      prop = null;

      if (UserGroupInformation.isSecurityEnabled()) {
        LOG.info("Security enabled, trying kerberoes login principal ["
            + principal + "] keytab [" + keytab + "]");
        //krb enabled
        if (principal != null && keytab != null) {
          SecureLoginUtil.login(KRB_PRINCIPAL, principal, KEY_TAB_FILE, keytab);
        }
        else  {
          LOG.error("Kerberoes principal/keytab not defined properly in " +
          "conduit.cfg");
          throw new RuntimeException("Kerberoes principal/keytab not defined " +
          "properly in conduit.cfg");
        }
      }

      ConduitConfigParser configParser =
      new ConduitConfigParser(conduitConfigFile);
      ConduitConfig config = configParser.getConfig();
      StringBuffer conduitClusterId = new StringBuffer();
      Set<String> clustersToProcess = new HashSet<String>();
      if (clusters.length == 1 && "ALL".equalsIgnoreCase(clusters[0])) {
        for (Cluster c : config.getClusters().values()) {
          clustersToProcess.add(c.getName());
        }
      } else {
        for (String c : clusters) {
          if (config.getClusters().get(c) == null) {
            LOG.warn("Cluster name is not found in the config - " + c);
            return;
          }
          clustersToProcess.add(c);
          conduitClusterId.append(c);
          conduitClusterId.append("_");
        }
      }
      final Conduit conduit = new Conduit(config, clustersToProcess);
      LOG.info("Starting CuratorLeaderManager for eleader election ");
      CuratorLeaderManager curatorLeaderManager =
      new CuratorLeaderManager(conduit, conduitClusterId.toString(),
      zkConnectString);
      curatorLeaderManager.start();
      Signal.handle(new Signal("INT"), new SignalHandler() {
        @Override
        public void handle(Signal signal) {
          try {
            LOG.info("Starting to stop conduit...");
            conduit.stop();
          }
          catch (Exception e) {
            LOG.warn("Error in shutting down conduit", e);
          }
        }
      });
    }
    catch (Exception e) {
      LOG.warn("Error in starting conduit daemon", e);
      throw new Exception(e);
    }
  }

}
