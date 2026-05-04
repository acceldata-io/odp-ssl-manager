
##Tool to enable SSL for the services in HDP stack

Steps to run the SSL Manager: (on ambari-server node) 

- Update ca.properties with java home, CA properties, Keystore & truststore passwords and the hostnames of cluster.
- To enable ssl for all services and ui's :
`./bin/ssl_manager.py --ca --properties=conf/ca.properties --scpKeyFile=<pem> --enable-ssl --service=all --ui=all  --host <ambari-host> --cluster <clustername>`

*Example:*
`cd ssl_manager`

`./bin/ssl_manager.py --ca --properties=conf/ca.properties --scpKeyFile=/root/.ssh/id_rsa --enable-ssl --service=all --ui=all --host hdp2005.qe.iti.acceldata.dev --cluster ad_hdp_automation_qe`

- To disable ssl for all services and ui's :
`./bin/ssl_manager.py --disable-ssl --host <ambari-host> --cluster <clustername> --service all --ui all`



``` Usage: ssl_manager.py [options] arg1

Options:
  -h, --help            show this help message and exit
  -v, --verbose
  --ca                  Create a CA using tls toolkit.
  --properties=PROPERTIES
                        ca.properties file which is used to create a CA.
  --isOverwrite         Overwrite existing certificates.
  --scpKeyFile=SCPKEYFILE
                        sshkey to copy the certificates to all the hosts.
  --scpUserName=SCPUSERNAME
                        username to copy the certificates to all the hosts.
                        Default is current user.
  --crtChown=CRTCHOWN   Ownership of all the certificates to all the hosts.
                        Default is 'root:hadoop'
  --enable-ssl          Enables ssl for HDP stack.
  --disable-ssl         Disables ssl for HDP stack.
  --service=SERVICE     Comma separated list of services for which SSL needs
                        to be enabled.'all' or comma seperated services.
                        Available configs are: HDFS,MRSHUFFLE,TEZ,HIVE,KAFKA,S
                        PARK,SPARK2,RANGERADMIN,RANGERPLUGINS
  --ui=UI               Comma separated list of UI's for which SSL needs to be
                        enabled. 'all' or comma seperated uis. Available ui's
                        are: HDFSUI,YARN,MAPREDUCE2UI,HBASE,OOZIE,AMBARI_INFRA
                        ,AMBARI_INFRA_SOLR,ATLAS,ZEPPELIN,STORM,AMBARI.
  --user=USER           Optional user ID to use for ambari authentication.
                        Default is 'admin'
  --password=PASSWORD   Optional password to use for ambari authentication.
                        Default is 'admin'
  --port=PORT           Optional port number for Ambari server. Default is
                        '8080'.Provide empty string to not use port.
  --protocol=PROTOCOL   Ambari server protocol. Default protocol is 'http'
  --host=HOST           Ambari Server external host name
  --cluster=CLUSTER     Name given to cluster. Ex: 'c1'
```

##How to Build

- Make sure you are in ssl_manager directory : `cd ssl_manager`
- Build using : 
`mvn clean install -DskipTests`
- ssl_manager-\<version>-all.tar will be generated under target directory: 
`target/ssl_manager-1.5.0-all.tar`



#### Install maven 3.1.0 higher version.
```
wget https://dlcdn.apache.org/maven/maven-3/3.8.6/binaries/apache-maven-3.8.6-bin.tar.gz
tar xf apache-maven-3.8.6-bin.tar.gz -C /opt
ln -s /opt/apache-maven-3.8.6 /opt/maven
cat <<EOF >/etc/profile.d/maven.sh
export M2_HOME=/opt/maven
export MAVEN_HOME=/opt/maven
export PATH=${M2_HOME}/bin:${PATH}
EOF
chmod +x /etc/profile.d/maven.sh
source /etc/profile.d/maven.sh
mvn -version
mvn clean install -DskipTests
cd ssl_manager
```
- ssl_manager-\<version>-all.tar will be generated under target directory: 
untar and run the cmd
