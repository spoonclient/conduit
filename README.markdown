# What is Conduit?
  Streaming data transport and collection system

# License
Copyright 2012 InMobi

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

# Goals
  * Provide a global & local view of data streams generating in different
  clusters and data centers.
  * Improved WAN bandwidth utilization by streaming data in incremental steps
  avoiding sudden bursts/spikes.
  * No loss of data once it reaches local cluster HDFS
  * Provide hot-hot fail over for all data when data center/cluster goes down.
  * Ability to have single primary and multiple mirrors of a stream across
  different clusters/data centers.

# Assumptions
  * Latency of 1-2 minutes in local data center, 2-3 minutes in remote data
  center and 4-5 minutes in the mirrored is acceptable.
  * All clusters across data centers should run in the same TIME ZONE. Conduit
   uses time as paths for files and for mirroring/merging we need same paths across D.C.'s.
   Recommendation - UTC.

# Concepts
  * Stream - A continuous data stream having similar data/records.
  * Local Stream - Minute files compressed on hadoop produced in local data
  centers
  * Merged Stream - Global view of all minute files generated in different
  clusters for same stream
  * Mirrored Stream - Copy of Merged Stream with paths being preserved.
                    
# How to build
  * mvn clean package -DskipTests=true

# How to use Conduit
  * Install the deb from $CONDUIT_HOME/conduit-dist/target/conduit-dist_$VERSION.deb
  * configure conduit.xml
  * configure conduit.cfg
  * Starting - . conduit.sh start conduit.cfg
  * Stopping - . conduit.sh stop conduit.cfg



