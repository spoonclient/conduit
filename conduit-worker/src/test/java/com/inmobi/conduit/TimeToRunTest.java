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

import org.apache.log4j.Logger;

import com.inmobi.conduit.local.LocalStreamService;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.testng.annotations.Test;

public class TimeToRunTest {
  
  private static Logger LOG = Logger.getLogger(TimeToRunTest.class);

  @Test
  public void getMSecondsTillNextRun() {
    
    Date date = new Date();
    SimpleDateFormat format = new SimpleDateFormat("yyyy:MM:dd:HH:mm:ss:SS");
    LOG.info("Test Date [" + format.format(date) + "]");
    try {
      LocalStreamService service = new LocalStreamService(null,
          ClusterTest.buildLocalCluster(), null);
      
      long mSecondsTillNextMin = service.getMSecondsTillNextRun(date.getTime());
      LOG.info("mSecondsTillNextMin = [" + mSecondsTillNextMin + "]");
      long roundedDate = date.getTime() + mSecondsTillNextMin;
      LOG.info("Rounded Date to next MIN [" + format.format(roundedDate) + "]");
      assert (roundedDate % 60000) == 0;
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
  }

}
