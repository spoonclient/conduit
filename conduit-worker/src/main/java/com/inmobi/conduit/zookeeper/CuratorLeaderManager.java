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
package com.inmobi.conduit.zookeeper;

import com.inmobi.conduit.Service;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.leader.LeaderSelector;
import com.netflix.curator.framework.recipes.leader.LeaderSelectorListener;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.retry.RetryOneTime;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetAddress;

public class CuratorLeaderManager implements LeaderSelectorListener {
  private static final Log LOG = LogFactory.getLog(CuratorLeaderManager.class);
  private final Service conduit;
  private final String conduitClusterId;
  private final String zkConnectString;
  private CuratorFramework client;
  private LeaderSelector leaderSelector;

  public CuratorLeaderManager(Service conduit, String conduitClusterId,
      String zkConnectString){
    this.conduit = conduit;
    this.conduitClusterId = conduitClusterId;
    this.zkConnectString = zkConnectString;
  }

  public void start() throws Exception {
    String zkPath = "/conduit/" + conduitClusterId; 
    this.client = CuratorFrameworkFactory.newClient(zkConnectString,
        new RetryOneTime(3));
    this.leaderSelector = new LeaderSelector(client, zkPath, this);
    connect();
  }

  private void connect() throws Exception {
    client.start();// connect to ZK
    LOG.info("becomeLeader :: connect to ZK [" + zkConnectString + "]");
    leaderSelector.setId(InetAddress.getLocalHost().getHostName());
    leaderSelector.start();
    LOG.info("started the LeaderSelector");
  }

  @Override
  public void takeLeadership(CuratorFramework curatorFramework)
      throws Exception {
    LOG.info("Became Leader..starting to do work");
 // This method shouldn't return till you want to release leadership
    conduit.start();
  }

  @Override
  public void stateChanged(CuratorFramework curatorFramework,
      ConnectionState connectionState) {
    if (connectionState == ConnectionState.LOST) {
      try {
        conduit.stop();
      } catch (Exception e1) {
        LOG.warn("Error while stopping conduit service", e1);
      }
      LOG.info("Releasing leadership..connection LOST");
      try {
        LOG.info("Trying reconnect..");
        connect();
      } catch (Exception e) {
        LOG.warn("Error in attempting to connect to ZK after releasing leadership", e);
      }
    }
  }

  

}
