# BigData
Installing Hadoop :
Follow this link - https://districtdatalabs.silvrback.com/creating-a-hadoop-pseudo-distributed-environment

To disable user permissions in HDFS, edit hdfs-site.xml by adding the following lines -
<property>
  <name>dfs.permissions</name>
  <value>false</value>
</property>

Installing Hive :
1) It would be convenient if you've followed the Hadoop installation from the link provided above.
2) Download the appropriate version of Hive from - https://hive.apache.org/downloads.html
3) Unzip the archive.
4) Use sudo to move the unzipped folder to /srv
5) Create a symbolic link by calling it "hive".
7) Download Apache Derby from - https://db.apache.org/derby/derby_downloads.html
8) Unzip the archive.
9) Use sudo to move the unzipped folder to /srv
10) Create a symbolic link by calling it "derby".
11) Run 
	$ sudo gedit ~/.bashrc
12) Append the following to the end of the file -
	export HIVE_HOME="/srv/hive"
	export PATH=$PATH:$HIVE_HOME/bin
	export CLASSPATH=$CLASSPATH:/srv/hadoop/lib/*:.
	export CLASSPATH=$CLASSPATH:/srv/hive/lib/*:.
	export DERBY_HOME=/srv/derby
	export PATH=$PATH:$DERBY_HOME/bin
	export CLASSPATH=$CLASSPATH:$DERBY_HOME/lib/derby.jar:$DERBY_HOME/lib/derbytools.jar
13) Run 
	$ source ~/.bashrc
	$ cd $HIVE_HOME/conf
	$ cp hive-env.sh.template hive-env.sh
	$ sudo gedit hive-env.sh
14) Append the following line -
	export HADOOP_HOME=/srv/hadoop
15) Run
	$ mkdir $DERBY_HOME/data
	$ cd $HIVE_HOME/conf
	$ cp hive-default.xml.template hive-site.xml
	$ sudo gedit hive-site.xml
16) Add the following as a child of configuration element -
	<property>
	   <name>javax.jdo.option.ConnectionURL</name>
	   <value>jdbc:derby://localhost:1527/metastore_db;create=true </value>
	   <description>JDBC connect string for a JDBC metastore </description>
	</property>
17) Create a file in $HIVE_HOME/conf called jprox.properties and add the following -
	javax.jdo.PersistenceManagerFactoryClass =
	org.jpox.PersistenceManagerFactoryImpl
	org.jpox.autoCreateSchema = false
	org.jpox.validateTables = false
	org.jpox.validateColumns = false
	org.jpox.validateConstraints = false
	org.jpox.storeManagerType = rdbms
	org.jpox.autoCreateSchema = true
	org.jpox.autoStartMechanismMode = checked
	org.jpox.transactionIsolation = read_committed
	javax.jdo.option.DetachAllOnCommit = true
	javax.jdo.option.NontransactionalRead = true
	javax.jdo.option.ConnectionDriverName = org.apache.derby.jdbc.ClientDriver
	javax.jdo.option.ConnectionURL = jdbc:derby://hadoop1:1527/metastore_db;create = true
	javax.jdo.option.ConnectionUserName = APP
	javax.jdo.option.ConnectionPassword = mine
18) Run
	$ $HADOOP_HOME/bin/hadoop fs -mkdir /tmp 
	$ $HADOOP_HOME/bin/hadoop fs -mkdir /warehouse
	$ $HADOOP_HOME/bin/hadoop fs -chmod g+w /tmp 
	$ $HADOOP_HOME/bin/hadoop fs -chmod g+w /warehouse
19) Open $HIVE_HOME/conf/hive-site.xml and replace all occurrences of ${system:java.io.tmpdir}/${system:user.name} by /tmp/mydir
19) Start hive -
	$ cd $HIVE_HOME
	$ bin/hive

