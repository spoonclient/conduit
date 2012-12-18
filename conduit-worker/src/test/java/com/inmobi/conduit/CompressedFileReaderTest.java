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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.inmobi.conduit.Conduit;
import com.inmobi.conduit.utils.FileUtil;

import java.io.OutputStream;

public class CompressedFileReaderTest {
  private static Logger LOG = Logger.getLogger(Conduit.class);

  private void uncompress(String fileName) throws Exception{
    Configuration conf = new Configuration();
    FileSystem fs;
    fs = FileSystem.getLocal(conf);

    CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
    CompressionCodec codec = codecFactory.getCodec(new Path(fileName));
    if (codec == null) {
      System.out.println("cant find codec");
      System.exit(1);
    }
    LOG.info("Using compression codec [" + codec.toString() + "]");
    CompressionInputStream is = codec.createInputStream(fs.open(new Path
            (fileName)));
    OutputStream out = null;
    try {
      String outputURI = CompressionCodecFactory.removeSuffix(fileName,
              codec.getDefaultExtension());
      out = fs.create(new Path(outputURI + "-uncompressed"));
      org.apache.hadoop.io.IOUtils.copyBytes(is, out, conf);
    }
    finally {
      org.apache.hadoop.io.IOUtils.closeStream(out);
      IOUtils.closeStream(is);

    }
  }

  private void compress(String fileName) throws Exception{
    FileUtil.gzip(new Path(fileName), new Path(fileName + ".gz"), new Configuration());
  }

  public static void main(String[] args) throws Exception{
    try {
      CompressedFileReaderTest cft = new CompressedFileReaderTest();
      if (args[0] != null && args[0].equalsIgnoreCase("uncompress"))
        cft.uncompress(args[1]);
      else if (args[0] != null && args[0].equalsIgnoreCase("compress"))
        cft.compress(args[1]);
    }
    catch (Exception e) {
      LOG.warn("Error", e);
    }

  }
}


