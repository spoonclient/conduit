# What is Conduit?
  Streaming data transport and collection system

# Why do we need when scribe, kafka, flume and other system exist?
  Our requirements to have data in HDFS and capability to view the same
  stream across D.C.'s and support it's mirrors for BCP didn't exist in kafka
   and flume during our evaluation. Scribe supports capability to bring data
   to HDFS in minute files, however a global streaming view of data across
   datacenters and it's mirrors in different data centers didn't exist.
  Conduit uses Scribe as Data Relay. But can be used with other systems
  like Flume as well.

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



