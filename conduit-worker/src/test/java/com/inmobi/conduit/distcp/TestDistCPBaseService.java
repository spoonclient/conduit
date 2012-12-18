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

import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.distcp.MirrorStreamService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestDistCPBaseService  {
  private static Logger LOG = Logger.getLogger(TestDistCPBaseService.class);
  Path testRoot = new Path("/tmp/", this.getClass().getName());
  FileSystem localFs;
  Cluster cluster;
  MirrorStreamService service = null;
  String expectedFileName1 = "/tmp/com.inmobi.conduit.distcp" +
      ".TestDistCPBaseService/data-file1";
  String expectedFileName2 = "/tmp/com.inmobi.conduit.distcp" +
      ".TestDistCPBaseService/data-file2";
  Set<String> expectedConsumePaths = new HashSet<String>();

  @BeforeTest
  public void setUP() throws Exception {
    //create fs
    localFs = FileSystem.getLocal(new Configuration());
    localFs.mkdirs(testRoot);

    //create cluster
    Map<String, String> clusterConf = new HashMap<String, String>();
    clusterConf.put("hdfsurl", localFs.getUri().toString());
    clusterConf.put("jturl", "local");
    clusterConf.put("name", "conduitCluster");
    clusterConf.put("jobqueuename", "default");
    Set<String> sourceNames = new HashSet<String>();
    sourceNames.add("stream1");
    Cluster cluster = new Cluster(clusterConf, testRoot.toString(), null,
        sourceNames);

    //create service
    service = new MirrorStreamService(null, cluster,
        cluster);

    //create data
    createValidData();

    //expectedConsumePaths
    expectedConsumePaths.add("file:/tmp/com.inmobi.conduit.distcp" +
        ".TestDistCPBaseService/system/mirrors/conduitCluster/file-with-valid-data");
    expectedConsumePaths.add("file:/tmp/com.inmobi.conduit.distcp" +
        ".TestDistCPBaseService/system/mirrors/conduitCluster/file-with-junk-data");
    expectedConsumePaths.add("file:/tmp/com.inmobi.conduit.distcp" +
        ".TestDistCPBaseService/system/mirrors/conduitCluster/file1-empty");

  }

  //@AfterTest
  public void cleanUP() throws IOException{
    //cleanup testRoot
    localFs.delete(testRoot, true);
  }

  private void createInvalidData() throws IOException{
    localFs.mkdirs(testRoot);
    Path dataRoot = new Path(testRoot, service.getInputPath());
    localFs.mkdirs(dataRoot);
    //one empty file
    Path p = new Path(dataRoot, "file1-empty");
    localFs.create(p);

    //one file with data put invalid paths
    p = new Path(dataRoot, "file-with-junk-data");
    FSDataOutputStream out = localFs.create(p);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
    writer.write("junkfile-1\n");
    writer.write("junkfile-2\n");
    writer.close();

  }

  private void createValidData() throws IOException{
    Path dataRoot = new Path(testRoot, service.getInputPath());
    localFs.mkdirs(dataRoot);
    //create invalid data
    createInvalidData();

    //one valid & invalid data file
    Path data_file = new Path(testRoot, "data-file1");
    localFs.create(data_file);

    Path data_file1 = new Path(testRoot, "data-file2");
    localFs.create(data_file1);

    //one file with data and one valid path and one invalid path
    Path p = new Path(dataRoot, "file-with-valid-data");
    FSDataOutputStream  out = localFs.create(p);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
    writer.write(data_file.toString() +"\n");
    writer.write("some-junk-path\n");
    writer.write(data_file1.toString() + "\n");
    writer.close();
  }

  @Test(priority = 1)
  public void testPositive() throws Exception {
    Map<Path, FileSystem> consumePaths = new HashMap<Path, FileSystem>();
    List<String> result=new ArrayList<String>();
    String currentLine = "";
    Path p =  service.getDistCPInputFile(consumePaths, testRoot);
    LOG.info("distcp input [" + p + "]");
    FSDataInputStream in = localFs.open(p);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    while ((currentLine = reader.readLine()) != null) {
    	result.add(currentLine);
    }
    reader.close();
    //assert that the minuteFileName inside the valid file with data
    //matches our expectedFileName1
    assert result.contains(expectedFileName1);
    assert result.contains(expectedFileName2);

    //second line was junkpath which would be skipped instead the next valid
    // path in input should be present

    Set<String> resultSet = new HashSet<String>();
    //compare consumePaths with expectedOutput
    for (Path consumePath : consumePaths.keySet()) {
      //cant compare the path generated using timestamp
      //The final path on destinationCluster which contains all valid
      // minutefileNames has a suffix of timestamp to it
      if (!consumePath.toString().contains("file:/tmp/com.inmobi.conduit" +
          ".distcp.TestDistCPBaseService/conduitCluster")) {
        LOG.info("Path to consume [" + consumePath + "]");
        resultSet.add(consumePath.toString());
      }
    }
    assert resultSet.containsAll(expectedConsumePaths);
    assert consumePaths.size() == resultSet.size() + 1;
  }


  @Test(priority = 2)
  public void testNegative() throws Exception {
    cleanUP();
    createInvalidData();
    Map<Path, FileSystem> consumePaths = new HashMap<Path, FileSystem>();
    Path p =  service.getDistCPInputFile(consumePaths, testRoot);
    //since all data is invalid
    //output of this function should be null
    assert p == null;

  }

}
