#!/usr/bin/env python3.11
import platform
import re
import os
import io
import sys
import logging
import json
import glob
import base64
import subprocess
import configparser
from optparse import OptionParser
import configs  # configs.py from ambari


logger = logging.getLogger(__name__)

CA_DIR = os.getcwd()
CA_CONF_DIR = "conf"

#####
ALL_SERVICES = ['HDFS', 'MRSHUFFLE', 'TEZ', 'HIVE', 'KAFKA', 'KAFKA3', 'RANGER', 'RANGER_KMS', 'SPARK2', 'SPARK3','SPARK3_3_5_1','SPARK3_3_3_3','DRUID' ,'IMPALA' ,'OZONE' ,'CRUISE_CONTROL' ,'CRUISE_CONTROL3','FLINK','LIVY','LIVY3','HTTPFS','KAFKA_CONNECT','KAFKA3_CONNECT','JUPYTER','OOZIE','PINOT','KUDU','CLICKHOUSE','ZOOKEEPER']
RANGER = ['RANGERADMIN', 'RANGERPLUGINS']

ALL_UI = ['HDFSUI', 'YARN', 'MAPREDUCE2UI', 'HBASE', 'OOZIE', 'AMBARI_INFRA', 'AMBARI_INFRA_SOLR', 'ATLAS', 'ZEPPELIN', 'NIFI', 'NIFI_REGISTRY','AIRFLOW','REGISTRY','HTTPFS']
AMBARI = ['AMBARIUI']
#####

# Location of certificates across all nodes.
CERT_DIR = "/etc/security/certificates"


KEYSTORE = 'keystore.jks'
TRUSTSTORE = 'truststore.jks'
AMBARI_P12 = 'ambari-keystore.p12'
AMBARI_PEM = 'ambari-keystore.pem'
AMBARI_CRT = 'ambari-keystore.crt'
KEYSTORE_LOCATION = os.path.join(CERT_DIR, 'keystore.jks')
TRUSTSTORE_LOCATION = os.path.join(CERT_DIR, 'truststore.jks')
AMBARI_P12_LOCATION = os.path.join(CERT_DIR, 'ambari-keystore.p12')
P12_LOCATION = os.path.join(CERT_DIR, 'keystore.p12')
PEM_KEY_LOCATION = os.path.join(CERT_DIR, 'key.pem')
PEM_CERT_LOCATION = os.path.join(CERT_DIR, 'cert.pem')

keystorepassword = ""
truststorepassword = ""
accessor = ""
IS_HADOOP = ['HDFS', 'HDFSUI', 'YARN', 'MRSHUFFLE', 'MAPREDUCE2UI', 'TEZ', 'HBASE']

CA = \
    """
    Generated a CA and server certificates for all the hosts:

    - Copy keystore.jks and truststore.jks files from host directories to respective hosts at /etc/security/certificates
    - Change the permissions "chmod 750 -R /etc/security/certificates"
    - Change the ownership to root:hadoop "chown -R root:hadoop /etc/security/certificates"
    """

# #### wget -O /usr/odp/current/oozie-server/libext/ext-2.2.zip http://tiny.cloudera.com/oozie-ext-2.2
OOZIE_UI = \
    """
    Select Oozie > Configs, then select Advanced oozie-env and set the following properties (update the <password> below):

export OOZIE_HTTPS_PORT=11443
export OOZIE_HTTPS_KEYSTORE_FILE=/etc/security/certificates/keystore.jks
export OOZIE_HTTPS_KEYSTORE_PASS=<password>
export OOZIE_CLIENT_OPTS="${OOZIE_CLIENT_OPTS} -Doozie.connection.retry.count=5 -Djavax.net.ssl.trustStore=/etc/security/certificates/truststore.jks -Djavax.net.ssl.trustStorePassword=<password>"

    Login to Oozie server and run: su -l oozie -c "/usr/hdp/current/oozie-server/bin/oozie-setup.sh prepare-war -secure"

    Note: Make sure Ext JS library is Installed and UI is already enabled.
    """
DISABLE_OOZIE_UI = \
    """
    Select Oozie > Configs, then select Advanced oozie-env and remove the following properties:

export OOZIE_HTTPS_PORT=11443
export OOZIE_HTTPS_KEYSTORE_FILE=/etc/security/certificates/keystore.jks
export OOZIE_HTTPS_KEYSTORE_PASS=<password>
export OOZIE_CLIENT_OPTS="${OOZIE_CLIENT_OPTS} -Doozie.connection.retry.count=5 -Djavax.net.ssl.trustStore=/etc/security/certificates/truststore.jks -Djavax.net.ssl.trustStorePassword=<password>"

    Login to Oozie server and run: su -l oozie -c "/usr/hdp/current/oozie-server/bin/oozie-setup.sh prepare-war"

    Note: Make sure Ext JS library is Installed and UI is already enabled.
    """

def generate_ca(properties, host, isoverwrite):
    """
    Generated a CA and server certificates for all the provided hosts using Tls toolkit.\
    Please copy the keystore.jks and truststore.jks files under host directory to respective hosts at /etc/security/certificates/
    Change the permissions to '750' using "chmod 750 /etc/security/certificates/*"
    """
    java_home = read_conf_file(properties, "env", "JAVA_HOME")
    java = java_home+'/bin/java'
    logger.info("Using JAVA {0}...".format(java))

    try:
        os.path.exists(CA_DIR)
    except OSError:
        raise
    logger.info("Using {0} as base path.".format(CA_DIR))
    if os.path.exists(properties):
        ca_props = read_ca_conf_file(properties, "caprops")
        logger.debug("CA properties are:".format(ca_props))
        opdir = os.path.abspath(read_conf_file(properties, "caprops", "outputDirectory"))
        toolkit_cmd = [java, '-jar', '-Xms12m', '-Xmx24m', CA_DIR + '/lib/ssl_manager-1.5.0-jar-with-dependencies.jar'
                       , 'standalone', '--certificateAuthorityHostname', read_conf_file(properties, "caprops", "caName")]
        if isoverwrite is True:
            toolkit_cmd.append("--isOverwrite")
        create_ca = toolkit_cmd + ca_props
        logger.debug("tls toolkit args are : {0}".format(create_ca))
        cacmd = subprocess.Popen(create_ca)
        cacmd.communicate()
        returncode = cacmd.poll()
        if not returncode == 0:
            logger.error("Unable to execute: {0}".format(create_ca))
            sys.exit(1)
        generate_ambari_specific(properties, host, opdir)
    return


def generate_ambari_specific(properties, host, outputdirectory):
    ambari_host = host
    ambari_keystore = os.path.join(outputdirectory, ambari_host, 'keystore.jks')
    ambari_p12 = os.path.join(outputdirectory, ambari_host, 'ambari-keystore.p12')
    ambari_pem = os.path.join(outputdirectory, ambari_host, 'ambari-keystore.pem')
    ambari_crt = os.path.join(outputdirectory, ambari_host, 'ambari-keystore.crt')

    logger.info("Keystore is:{0}".format(ambari_keystore))
    logger.info("P12 is:{0}".format(ambari_p12))

    java_home = read_conf_file(properties, "env", "JAVA_HOME")
    keytool = java_home+'/bin/keytool'
    logger.info("Using Keytool {0}...".format(keytool))
    
    os_type, os_version = get_os_version()

    createp12 = [keytool, '-importkeystore', '-srckeystore', ambari_keystore,
                 '-destkeystore', ambari_p12, '-srcstoretype', 'jks',
                 '-deststoretype', 'pkcs12', '-srcstorepass', keystorepassword.strip(), '-deststorepass', keystorepassword, '-noprompt']
    createpem = ['openssl', 'pkcs12', '-in', ambari_p12, '-out', ambari_pem, '-passin',
                 'pass:'+keystorepassword, '-passout', 'pass:'+keystorepassword]
    createcrt = ['openssl', 'x509', '-in', ambari_pem, '-out', ambari_crt]
    
    # Use '-legacy' only if OS version is **above Rocky 8 or Ubuntu 20**
    use_legacy_flag = (os_type in ['10', '9'] and os_version >= 9) or (os_type in ['24','22','23'] and os_version >= 22)

    if use_legacy_flag:
        createpem.insert(2, '-legacy')  # Add '-legacy' flag at correct position

    logger.info("Creating ambari-keystore.p12 for ambari...")
    cmd = subprocess.Popen(createp12)
    cmd.communicate()
    returncode = cmd.poll()
    if not returncode == 0:
        logger.error("Unable to execute: {0}".format(createp12))
        sys.exit(1)
    logger.info("Creating ambari-keystore.pem for ambari...")
    cmd = subprocess.Popen(createpem)
    cmd.communicate()
    returncode = cmd.poll()
    if not returncode == 0:
        logger.error("Unable to execute: {0}".format(createpem))
        sys.exit(1)
    logger.info("Creating ambari-keystore.crt for ambari...")
    cmd = subprocess.Popen(createcrt)
    cmd.communicate()
    returncode = cmd.poll()
    if not returncode == 0:
        logger.error("Unable to execute: {0}".format(createcrt))
        sys.exit(1)
    return


def get_os_version():
    """Detects the OS and version."""
    try:
        os_info = platform.system().lower()
        if os_info == "linux":
            # Use 'cat /etc/os-release' to get OS details
            with open("/etc/os-release", "r") as f:
                os_release = f.read()

            # Extract OS ID and VERSION_ID
            os_type_match = re.search(r'ID="?(\w+)"?', os_release)
            os_version_match = re.search(r'VERSION_ID="?([\d.]+)"?', os_release)

            os_type = os_type_match.group(1) if os_type_match else "unknown"
            os_version = float(os_version_match.group(1)) if os_version_match else 0.0
            logger.info("OS VERSION of this machine:  {0}...{1}".format(os_type,os_version))
            return os_type, os_version
    except Exception as e:
        logger.error(f"Failed to determine OS version: {e}")
        return "unknown", 0.0


def read_service_configs(service_name, conf_file):
    ssl_configs = ""
    try:
        os.path.exists(conf_file)
        with open(conf_file) as f:
            config = yaml.safe_load(f)
    except OSError as e:
        logger.error(e)
        return 1
    if service_name in list(config.keys()):
        logger.info("Reading SSL configs for service:{0}".format(service_name))
        ssl_configs = config[service_name]
    else:
        logger.warn("Unable to find SSL configs for: {0} in {1}".format(service_name, conf_file))
        logger.warn("Available configs are: {0}".format(list(config.keys())))
    #    Consider only installed ranger plugins
    if service_name == "RANGERPLUGINS":
        plugins_to_be_considered = []
        for i in ranger_services_to_be_considered:
            plugins_to_be_considered.append('ranger-'+i.lower()+'-policymgr-ssl')
        for i in ranger_ui_to_be_considered:
            plugins_to_be_considered.append('ranger-'+i.lower()+'-policymgr-ssl')
        ssl_configs = [plugins for plugins in ssl_configs if plugins['config_type'] in plugins_to_be_considered]
    return ssl_configs


def get_configs(accessor, cluster, config_type):
    try:
        properties, attributes = configs.get_current_config(cluster, config_type, accessor)
    except KeyError:
        # This is to fix empty ranger-site
        properties, attributes = {}, {}
    config = properties, attributes
    logger.debug("Got configs from Ambari for {0}: {1}".format(config_type, json.dumps(config, indent=2)))
    return config


def put_configs(config):
    def update(cluster, config_type, accessor):
        if config[0] is None:
            config[0] = {}
        if config[1] is None:
            config[1] = {}
        new_properties = config[0]
        new_attributes = config[1]
        logger.info('### PUTting : "{0}"'.format(json.dumps(config, indent=2)))
        return new_properties, new_attributes
    return update


def get_password(properties, pwd_type):
    password = ""
    if pwd_type is "keyStorePassword":
        password = base64.b64decode(read_conf_file(properties, "caprops", "keyStorePassword")).decode('utf-8')
    elif pwd_type is "trustStorePassword":
        password = base64.b64decode(read_conf_file(properties, "caprops", "trustStorePassword")).decode('utf-8')
    return password


def update_configs_ambari(services, accessor, cluster, conf_file):
    config = {}
    for s_name in services.split(','):
        logger.debug("Reading SSL configs from {0}".format(conf_file))
        ssl_configs = read_service_configs(s_name.upper(), conf_file)
        logger.debug("ssl_configs for {0} are {1}".format(s_name.upper(), ssl_configs))
        for section in ssl_configs:
            config_type = section['config_type']
            del section['config_type']
            try:
                config = get_configs(accessor, cluster, config_type)
            except Exception:
                logger.warn("Unable to get configs for config_type:{0} from Ambari".format(config_type))
                return 1
            for k in section:
                if section[k] == "$keystore":
                    section[k] = KEYSTORE_LOCATION
                elif section[k] == "$truststore":
                    section[k] = TRUSTSTORE_LOCATION
                elif section[k] == "$ambari_p12":
                    section[k] = AMBARI_P12_LOCATION
                elif section[k] == "$pemKey":
                    section[k] = PEM_KEY_LOCATION
                elif section[k] == "$pemCert":
                    section[k] = PEM_CERT_LOCATION
                elif section[k] == "$p12_location":
                    section[k] = P12_LOCATION
                elif section[k] == "$keystorepassword":
                    section[k] = keystorepassword
                elif section[k] == "$truststorepassword":
                    section[k] = truststorepassword
                elif section[k] == "$historyserver":
                    section[k] = config[0].get("yarn.log.server.url").replace('http:', 'https:').replace('19888', '19890')
                elif section[k] == "$timelineserver":
                    section[k] = config[0].get("yarn.log.server.web-service.url").replace('http:', 'https:').replace('8188', '8190')
                elif section[k] == "$oozieserver":
                    section[k] = config[0].get("oozie.base.url").replace('http:', 'https:').replace('11000', '11443')
                elif section[k] == "$druidLoadList":
                    section[k] = config[0].get("druid.extensions.loadList").replace(']', ',"simple-client-sslcontext"]') 
                elif section[k] == "$rangerSslUrl":
                    section[k] = config[0].get("policymgr_external_url").replace('http:', 'https:').replace('6080','6182')
                elif section[k] == "$keyProviderPath":
                    section[k] = config[0].get("hadoop.security.key.provider.path").replace('http:', 'https:').replace('9292','9393')
                elif section[k] == "$keyProviderUri":
                    section[k] = config[0].get("dfs.encryption.key.provider.uri").replace('http:', 'https:').replace('9292','9393')
                elif section[k] == "$rangerPolicyUrl":
                    # Find all matching keys using regex pattern
                    for config_key in config[0]:
                        if re.match(r".*\.policy\.rest\.url$", config_key):
                            section[k] = config[0][config_key].replace('http:', 'https:').replace('6080', '6182')
                   
                config[0].update({k: section[k]})
            updater = put_configs(config)
            configs.update_config(cluster, config_type, updater, accessor)
            logger.info("Updated configurations for service {0}[{1}]".format(s_name, config_type))
    return


def disable_configs(service, accessor, cluster, conf_file):
    logger.debug("Reading SSL configs from {0}".format(conf_file))
    ssl_configs = read_service_configs(service, conf_file)
    logger.debug("ssl_configs for {0} are {1}".format(service, ssl_configs))

    for section in ssl_configs:
        config_type = section['config_type']
        keys = list(section.keys())
        del section['config_type']
        if "delete" in keys:
            del section['delete']
            for k in section:
                try:
                    configs.update_config(cluster, config_type, configs.delete_specific_property(k), accessor)
                except Exception:
                    logger.warn("Unable to get/delete configs for config_type:{0} from Ambari".format(config_type))
                    return 1
                logger.info("Disabled SSL for service {0}[{1}]".format(service, config_type))
        else:
            try:
                config = get_configs(accessor, cluster, config_type)
            except Exception:
                logger.warn("Unable to get configs for config_type:{0} from Ambari".format(config_type))
                return 1
            for k in section:
                if section[k] == "$historyserver":
                    section[k] = config[0].get("yarn.log.server.url").replace('https:', 'http:').replace('19890', '19888')
                elif section[k] == "$timelineserver":
                    section[k] = config[0].get("yarn.log.server.web-service.url").replace('https:', 'http:').replace('8190', '8188')
                config[0].update({k: section[k]})
            logger.debug("New configs for {0} are :{1}".format(config_type, json.dumps(config, indent=2)))
            updater = put_configs(config)
            configs.update_config(cluster, config_type, updater, accessor)
            logger.info("Disabled SSL for service {0}[{1}]".format(service, config_type))
    return



def get_remote_os_type(ssh_key, userhost):
    try:
        command = "cat /etc/os-release | grep '^ID='"
        ssh_command = ['ssh', '-o', 'StrictHostKeyChecking=no', '-i', ssh_key, userhost, command]
        
        # No need for shell=True when passing a list of arguments
        result = subprocess.Popen(ssh_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = result.communicate()  # capture the output and error
        
        if result.returncode != 0:
            logger.error("Failed to get OS type for host {0}: {1}".format(userhost, stderr))
            return None
        
        if b'ubuntu' in stdout:
            return 'ubuntu'
        elif b'rhel' in stdout or b'centos' in stdout:
            return 'rhel'
        elif b'rocky' in stdout:
            return 'rocky'
        else:
            logger.error("Unrecognized OS type from host {0}".format(userhost))
            return None
    
    except Exception as e:
        logger.error("Exception occurred: {0}".format(str(e)))
        return None


def execute_remote_commands(ssh_key, userhost, export_command, delete_command_cacerts, delete_command_ambari,import_command_cacerts, import_command_ambari, create_pkcs12, create_pem_key, create_pem_cert, add_ambari_ssl_props):
    try:
        # Execute export cert command
        logger.info("Exporting cert from truststore on host {0}".format(userhost))
        subprocess.Popen(
            "ssh -o StrictHostKeyChecking=no -i {0} {1} '{2}'".format(ssh_key, userhost, export_command), 
            shell=True
        ).communicate()


        # Delete existing cert in cacerts if it exists
        logger.info("Deleting existing cert from cacerts on host {0}".format(userhost))
        subprocess.Popen(
            "ssh -o StrictHostKeyChecking=no -i {0} {1} '{2}'".format(ssh_key, userhost, delete_command_cacerts), 
            shell=True
        ).communicate()

        # Delete existing cert in Ambari truststore if it exists
        logger.info("Deleting existing cert from Ambari truststore on host {0}".format(userhost))
        subprocess.Popen(
            "ssh -o StrictHostKeyChecking=no -i {0} {1} '{2}'".format(ssh_key, userhost, delete_command_ambari), 
            shell=True
        ).communicate()

        # Execute import cert command for cacerts
        logger.info("Importing cert into cacerts on host {0}".format(userhost))
        subprocess.Popen(
            "ssh -o StrictHostKeyChecking=no -i {0} {1} '{2}'".format(ssh_key, userhost, import_command_cacerts), 
            shell=True
        ).communicate()

        # Execute import cert command for Ambari truststore
        logger.info("Importing cert into Ambari truststore on host {0}".format(userhost))
        subprocess.Popen(
            "ssh -o StrictHostKeyChecking=no -i {0} {1} '{2}'".format(ssh_key, userhost, import_command_ambari), 
            shell=True
        ).communicate()


        # Create pkcs12 file to extract key and cert for impala, airflow ssl
        logger.info("Create pkcs12 file on host {0}".format(userhost))
        subprocess.Popen(
            "ssh -o StrictHostKeyChecking=no -i {0} {1} '{2}'".format(ssh_key, userhost, create_pkcs12),
            shell=True
        ).communicate()


        # Create pem key file for SSL enablement
        logger.info("Create pem key file on host {0}".format(userhost))
        logger.info("cmd for PEM : {0}".format(create_pem_key))
        subprocess.Popen(
            "ssh -o StrictHostKeyChecking=no -i {0} {1} '{2}'".format(ssh_key, userhost, create_pem_key),
            shell=True
        ).communicate()

        # Create pem cert file for SSL enablement
        logger.info("Create pem cert file on host {0}".format(userhost))
        logger.info("cmd for PEM : {0}".format(create_pem_cert))
        subprocess.Popen(
            "ssh -o StrictHostKeyChecking=no -i {0} {1} '{2}'".format(ssh_key, userhost, create_pem_cert),
            shell=True
        ).communicate()
        
        # Append ambari configs
        logger.info("Append ambari_ssl props on host {0}".format(userhost))
        subprocess.Popen(
            "ssh -o StrictHostKeyChecking=no -i {0} {1} '{2}'".format(ssh_key, userhost, add_ambari_ssl_props),
            shell=True
        ).communicate()
        
    except Exception as e:
        logger.error("Failed to execute commands on host {0}: {1}".format(userhost, str(e)))

def execute_remote_bash(cmd):
    try:
        # Execute export cert command
        logger.info("Executing command {0} on host {1}".format(cmd, userhost))
        subprocess.Popen(
            "ssh -o StrictHostKeyChecking=no -i {0} {1} '{2}'".format(ssh_key, userhost, cmd), 
            shell=True
        ).communicate()
    except Exception as e:
        logger.error("Failed to execute commands on host {0}: {1}".format(userhost, str(e)))

def find_file_path(base_dir, filename):
    for root, dirs, files in os.walk(base_dir):
        if filename in files:
            return os.path.join(root, filename)
    return None

def copy_certs(properties, ssh_key, scpusername, ownership):
    opdir = os.path.abspath(read_conf_file(properties, "caprops", "outputDirectory"))
    host_list = read_conf_file(properties, "caprops", "hostnames")
    ssh_key = os.path.expanduser(ssh_key)

    for host in host_list.split(','):
        logger.info(host)
        source = os.path.join(opdir, host) + '/*'
        ambari_keystore_path = find_file_path(opdir, "ambari-keystore.p12")
        dest = scpusername + '@' + host + ':' + CERT_DIR + '/'
        userhost = scpusername + '@' + host
        scp_command = "scp -o StrictHostKeyChecking=no -i " + ssh_key + " " + source + " " + dest
        scp_ambari_keystore = "scp -o StrictHostKeyChecking=no -i " + ssh_key + " " + ambari_keystore_path + " " + dest

        os_type, os_version = get_os_version()
        # Use '-legacy' only if OS version is **above Rocky 8 or Ubuntu 20**
        use_legacy_flag = (os_type in ['10', '9'] and os_version >= 9) or (os_type in ['24','22','23'] and os_version >= 22)

        legacy_option = "-legacy" if use_legacy_flag else ""

        logger.info("Creating cert dir {0} in host {1}".format(CERT_DIR, host))
        subprocess.Popen(['ssh', '-o', 'StrictHostKeyChecking=no', '-i', ssh_key, userhost, 'mkdir', '-p', CERT_DIR]).communicate()

        logger.info("Copying certs to host {0}".format(host))
        subprocess.Popen(scp_command, shell=True).communicate()

        logger.info("Copying ambari-keystore.p12 to host {0}".format(host))
        subprocess.Popen(scp_ambari_keystore, shell=True).communicate()

        logger.info("Changing the ownership of certificates..")
        subprocess.Popen(['ssh', '-o', 'StrictHostKeyChecking=no', '-i', ssh_key, userhost, 'chown', '-R', ownership, CERT_DIR]).communicate()

        logger.info("Changing the permissions on directory..")
        subprocess.Popen(['ssh', '-o', 'StrictHostKeyChecking=no', '-i', ssh_key, userhost, 'chmod', '755', CERT_DIR]).communicate()

        logger.info("Changing the permissions on certificate files..")
        subprocess.Popen(['ssh', '-o', 'StrictHostKeyChecking=no', '-i', ssh_key, userhost, 'chmod', '644', CERT_DIR + '/*']).communicate()

        create_pkcs12 = "keytool -importkeystore -srckeystore " + CERT_DIR + '/' + "keystore.jks -destkeystore " + CERT_DIR + '/' + "keystore.p12 -srcstoretype jks -deststoretype pkcs12 -srcstorepass " + keystorepassword + " -deststorepass " + keystorepassword + " -destkeypass " + keystorepassword + " -alias nifi-cert -noprompt && chmod 644 " + CERT_DIR + "/keystore.p12"

        create_pem_key = "openssl pkcs12 {0} -in {1}/keystore.p12 -nocerts -out {1}/key.pem -nodes -passin pass:{2} && chmod 644 {1}/key.pem".format(legacy_option, CERT_DIR, keystorepassword.strip())

        create_pem_cert = "openssl pkcs12 {0} -in {1}/keystore.p12 -nokeys -out {1}/cert.pem -passin pass:{2} && chmod 644 {1}/cert.pem".format(legacy_option, CERT_DIR, keystorepassword.strip())


        # Determine the OS type dynamically
        os_type = get_remote_os_type(ssh_key, userhost)

        if os_type:
            if os_type == 'ubuntu':
                logger.info("Running keytool commands for Ubuntu...")
                export_command = "keytool -exportcert -alias nifi-cert -keystore " + CERT_DIR + '/' + "truststore.jks -file /tmp/mycert.crt -storepass " + truststorepassword + " -noprompt"
                delete_command_cacerts = "keytool -delete -alias nifi-cert -keystore /etc/ssl/certs/java/cacerts -storepass changeit"
                delete_command_ambari = "keytool -delete -alias nifi-cert -keystore /etc/ambari-server/conf/truststore.jks -storepass changeit"

                import_command_cacerts = "keytool -importcert -alias nifi-cert -file /tmp/mycert.crt -keystore /etc/ssl/certs/java/cacerts -storepass changeit -noprompt"
                import_command_ambari = "keytool -importcert -alias nifi-cert -file /tmp/mycert.crt -keystore /etc/ambari-server/conf/truststore.jks -storepass changeit -noprompt"
                
            elif os_type in ['rhel', 'centos', 'rocky']:
                logger.info("Running keytool commands for Rocky Linux or RHEL...")
                export_command = "keytool -exportcert -alias nifi-cert -keystore /etc/security/certificates/truststore.jks -file /tmp/mycert.crt -storepass Hadoop@123 -noprompt"
                delete_command_cacerts = "keytool -delete -alias nifi-cert -keystore /etc/pki/ca-trust/extracted/java/cacerts -storepass changeit"
                delete_command_ambari = "keytool -delete -alias nifi-cert -keystore /etc/ambari-server/conf/truststore.jks -storepass changeit"

                import_command_cacerts = "keytool -importcert -alias nifi-cert -file /tmp/mycert.crt -keystore /etc/pki/ca-trust/extracted/java/cacerts -storepass changeit -noprompt"
                import_command_ambari = "keytool -importcert -alias nifi-cert -file /tmp/mycert.crt -keystore /etc/ambari-server/conf/truststore.jks -storepass changeit -noprompt"

            add_ambari_ssl_props = "echo 'ssl.trustStore.path=/etc/security/certificates/truststore.jks' >> /etc/ambari-server/conf/ambari.properties && echo 'ssl.trustStore.password=Hadoop@123' >> /etc/ambari-server/conf/ambari.properties"
            # Execute the remote commands
            execute_remote_commands(ssh_key, userhost, export_command, delete_command_cacerts, delete_command_ambari, import_command_cacerts, import_command_ambari, create_pkcs12, create_pem_key, create_pem_cert, add_ambari_ssl_props)
        else:
            logger.error("Could not determine OS type for host {0}. Skipping keytool operations.".format(userhost))


    return



def read_ca_conf_file(properties, section):
    """
    :param properties: property file
    :param section: section name
    :return: Returns the list of key-value pairs in a given section
    """
    ca_props = []
    keypass = keystorepassword
    trustpass = truststorepassword
    if os.path.exists(properties):
        with open(properties) as f:
            ca_config = f.read()
        config = configparser.RawConfigParser()
        config.optionxform = str
        config.readfp(io.StringIO(ca_config))
        for options in config.options(section):
            ca_props.append("--" + options)
            config.set(section, "keyStorePassword", keypass)
            config.set(section, "trustStorePassword", trustpass)
            ca_props.append(config.get(section, options))
    return ca_props


def read_conf_file(properties, section, key):
    """
    :param properties: property file
    :param section: section name
    :param key: key
    :return: Returns the value of a key in a given section
    """
    value = ""
    if os.path.exists(properties):
        with open(properties) as f:
            ca_config = f.read()  # ca_config is a string here
        config = configparser.RawConfigParser()
        config.optionxform = str  # Ensure case-sensitive parsing
        config.read_file(io.StringIO(ca_config))  # Use StringIO to parse the string content
        value = config.get(section, key)  # Example key, adjust as needed
    return value


def delete_properties(cluster, config_type, args, accessor):
    logger.info('### Performing "delete":')
    if len(args) == 0:
        logger.error("Not enough arguments. Expected config key.")
        return -1

    config_name = args[0]
    logger.info('### on property "{0}"'.format(config_name))
    configs.update_config(cluster, config_type, configs.delete_specific_property(config_name), accessor)
    return 0


def disable_service(services, cluster, conf_file):
    for servicename in services:
        s_name = servicename.upper()
        logger.info(s_name)
        if s_name == 'ALL':
            for i in services_to_be_considered:
                logger.info("Disabling ssl for {0}".format(i))
                disable_configs(i.upper(), accessor, cluster, conf_file)
        else:
            disable_configs(s_name, accessor, cluster, conf_file)
    return


def disable_ui(uis, cluster, conf_file):
    for uiname in uis:
        u_name = uiname.upper()
        logger.info(u_name)
        if u_name == 'ALL':
            for i in ui_to_be_considered:
                if i == "AMBARIUI":
                    logger.info("Disabling ssl for {0}".format(i))
                    subprocess.Popen(disable_ambari_ui()).communicate()
                elif i == "OOZIE":
                    logger.info("Please follow below instructions to disable SSL for Oozie.")
                    logger.info(DISABLE_OOZIE_UI)
                else:
                    logger.info("Disabling ssl for {0}".format(i))
                    disable_configs(i.upper(), accessor, cluster, conf_file)
        else:
            if u_name == "AMBARIUI":
                subprocess.Popen(disable_ambari_ui()).communicate()
            elif u_name == "OOZIE":
                logger.info("Please follow below instructions to disable SSL for Oozie.")
                logger.info(DISABLE_OOZIE_UI)
            else:
                disable_configs(u_name, accessor, cluster, conf_file)
    return


def parse_service(services, accessor, cluster, conf_file):
    for servicename in services:
        s_name = servicename.upper()
        logger.info(s_name)
        if s_name == 'ALL':
            for i in services_to_be_considered:
                logger.info("Enabling SSL for {0}".format(i))
                update_configs_ambari(i.upper(), accessor, cluster, conf_file)
        else:
            #if u_name == '':
            update_configs_ambari(s_name, accessor, cluster, conf_file)
    return


def parse_ui(uis, accessor, cluster, conf_file):
    for uiname in uis:
        u_name = uiname.upper()
        logger.info(u_name)
        if u_name == 'ALL':
            for i in ui_to_be_considered:
                logger.info("Enabling SSL for {0}".format(i))
                if i.upper() == "AMBARIUI":
                    subprocess.Popen(enable_ambari_ui()).communicate()
                elif i.upper() == 'OOZIE':
                    logger.info("Configs to update in {0} are: {1}".format(i.upper(), OOZIE_UI))
                elif i.upper() == 'ATLAS':
                    update_configs_ambari(i.upper(), accessor, cluster, conf_file)
                    logger.info("Perform below operation to enable ssl for {0}: {1}".format(i.upper(), ATLAS_UI))
                else:
                    update_configs_ambari(i.upper(), accessor, cluster, conf_file)

        else:
            if u_name == 'AMBARIUI':
                subprocess.Popen(enable_ambari_ui()).communicate()
                logger.info("Enabling SSL for {0} using : {1}".format(u_name, enable_ambari_ui()))
            elif u_name == 'OOZIE':
                logger.info("Configs to update in {0} are: {1}".format(u_name, OOZIE_UI))
            elif u_name == 'ATLAS':
                update_configs_ambari(u_name, accessor, cluster, conf_file)
                logger.info("Perform below operation to enable ssl for {0}: {1}".format(u_name, ATLAS_UI))
            else:
                logger.info("Enabling SSL for {0}".format(u_name))
                update_configs_ambari(u_name, accessor, cluster, conf_file)
    return


def select_config_file(accessor, conf_type):
    installed_ambari_version = configs.get_ambari_version(accessor).replace('.', '')
    available_config_files = [os.path.basename(x) for x in glob.glob(os.path.join(CA_DIR, CA_CONF_DIR, conf_type+'*'))]
    available_config_versions = [i.replace(conf_type, '').replace('.yaml', '') for i in available_config_files]
    available_config_versions.sort(reverse=True)
    logger.debug("Installed ambari version is: {0}".format(installed_ambari_version))
    logger.debug("Available conf files are: {0}".format(available_config_versions))
    version = compare_versions(available_config_versions, installed_ambari_version)
    conf_file = ""
    if version is not None:
        conf_file = conf_type+str(version)+'.yaml'
        logger.info("Using config file :{0}".format(os.path.join(CA_CONF_DIR, conf_file)))
    else:
        logger.error("Unable to find appropriate config file for version {0}.".format(installed_ambari_version))
        exit(1)
    return conf_file


def compare_versions(available_versions, current_version):
    for x in available_versions:
        if current_version == x:
            version = x
            return version
        elif current_version > x:
            version = x
            return version
    return


def is_hadoop_required(a, b):
    return any([i in b for i in a])


def enable_ambari_ui():
    ambari_ui = ['ambari-server', 'setup-security', '--security-option=setup-https', '--api-ssl=true',
                 '--api-ssl-port=8446', '--import-cert-path='+os.path.join(CERT_DIR, AMBARI_CRT),
                 '--import-key-path='+os.path.join(CERT_DIR, AMBARI_PEM), '--pem-password='+keystorepassword]
    return ambari_ui


def disable_ambari_ui():
    ambari_ui = ['ambari-server', 'setup-security', '--security-option=setup-https', '--api-ssl=false']
    return ambari_ui


def main():
    parser = OptionParser(usage="usage: %prog [options] arg1", )
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default="False")
    parser.add_option("--ca", action="store_true", default=False, dest="ca",
                      help="Create a CA using tls toolkit.")
    parser.add_option("--properties", dest="properties",
                      help="ca.properties file which is used to create a CA.")
    parser.add_option("--isOverwrite", action="store_true", default=False, dest="isOverwrite",
                      help="Overwrite existing certificates.")
    parser.add_option("--scpKeyFile", dest="scpKeyFile",
                      help="sshkey to copy the certificates to all the hosts.")
    parser.add_option("--scpUserName", dest="scpUserName",
                      help="username to copy the certificates to all the hosts. Default is current user.", default=os.environ.get('USER'))
    parser.add_option("--crtChown", dest="crtChown",
                      help="Ownership of all the certificates to all the hosts. Default is 'root:hadoop'", default="root:hadoop")
    parser.add_option("--enable-ssl", action="store_true", default=False,
                      dest="enablessl", help="Enables ssl for HDP stack.")
    parser.add_option("--disable-ssl", action="store_true", default=False,
                      dest="disablessl", help="Disables ssl for HDP stack.")
    parser.add_option("--service", help="Comma separated list of services for which SSL "
                                        "needs to be enabled.'all' or comma seperated services. "
                                        "Available configs are: HDFS,MRSHUFFLE,TEZ,HIVE,KAFKA,SPARK,SPARK2,RANGERADMIN,RANGERPLUGINS")
    parser.add_option("--ui", dest="ui", help="Comma separated list of UI's for which SSL needs "
                                              "to be enabled. 'all' or comma seperated uis. "
                                              "Available ui's are: HDFSUI,YARN,MAPREDUCE2UI,HBASE,OOZIE,AMBARI_INFRA,AMBARI_INFRA_SOLR,ATLAS,ZEPPELIN,STORM,AMBARI,NIFI,NIFI_REGISTRY.")

    # Ambari arguments

    parser.add_option("--user", dest="user", default="admin",
                      help="Optional user ID to use for ambari authentication. Default is 'admin'")
    parser.add_option("--password", dest="password", default="admin",
                      help="Optional password to use for ambari authentication. Default is 'admin'")
    parser.add_option("--port", dest="port", default="8080",
                      help="Optional port number for Ambari server. Default is '8080'."
                           "Provide empty string to not use port.")
    parser.add_option("--protocol", dest="protocol", default="http",
                      help="Ambari server protocol. Default protocol is 'http'")
    parser.add_option("--host", dest="host", help="Ambari Server external host name")
    parser.add_option("--cluster", dest="cluster", help="Name given to cluster. Ex: 'c1'")

    (options, args) = parser.parse_args()

    if options.enablessl is False and options.disablessl is False:
        parser.error("wrong number of arguments,Option --enable-ssl or --disable-ssl is mandatory.")
    # if not (options.service or options.ui) or (options.ca is False):
    #     parser.error("wrong number of arguments")
    if not (options.service or options.ui):
        parser.error("Choose service or ui for which you wish to enable SSL.")
    if options.ca is True and not options.properties:
        parser.error("Along with --ca, you should pass ca.properties using --properties")
    if options.enablessl is True and not options.properties:
        parser.error("Along with --enable-ssl, you should pass ca.properties using --properties")
    if None in [options.host, options.cluster]:
        parser.error("Ambari host / Cluster name are not passed")
    if options.scpKeyFile is not None and not os.path.exists(os.path.expanduser(options.scpKeyFile)):
        parser.error("{0} doesn't exists.".format(os.path.expanduser(options.scpKeyFile)))

    if options.verbose is True:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    logger.setLevel(loglevel)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(loglevel)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)

    logger.debug("In verbose mode...\nCli args are:{0}".format(options))

    ca = options.ca
    properties = options.properties
    isoverwrite = options.isOverwrite
    scpkeyfile = options.scpKeyFile
    scpusername = options.scpUserName
    crtchown = options.crtChown
    service = options.service
    ui = options.ui
    user = options.user
    password = options.password
    port = options.port
    protocol = options.protocol
    host = options.host
    cluster = options.cluster
    enable = options.enablessl
    disable = options.disablessl

    # Init global variables
    global accessor
    accessor = configs.api_accessor(host, user, password, protocol, port)

    global services_to_be_considered
    global ui_to_be_considered
    global ranger_ui_to_be_considered
    global ranger_services_to_be_considered

    conf_file_type = ""
    if enable is True:
        conf_file_type = "enable_configs"
    elif disable is True:
        conf_file_type = "disable_configs"
    else:
        logger.error("No config type configured...")
    conf_file = os.path.join(CA_CONF_DIR, select_config_file(accessor, conf_file_type))

    # Getting installed services from ambari and map with the configurable ones.
    installed_services = configs.get_installed_services(cluster, accessor)
    installed_services = installed_services + AMBARI
    if "MAPREDUCE2" in installed_services:
        installed_services.append("MRSHUFFLE")
        installed_services.append("MAPREDUCE2UI")
        installed_services.remove("MAPREDUCE2")
    if "RANGER" in installed_services:
        installed_services.remove("RANGER")
        installed_services = installed_services + RANGER
    if "HDFS" in installed_services:
        installed_services.append("HDFSUI")

    logger.debug("Installed Services/UI's on cluster are: {0}".format(installed_services))
    logger.debug("UI's ssl_manager can configure are: {0}".format(ALL_UI + AMBARI))
    logger.debug("Services ssl_manager can configure are: {0}".format(ALL_SERVICES + RANGER))


    logger.info("Installed Services/UI's on cluster are: {0}".format(installed_services))
    logger.info("UI's ssl_manager can configure are: {0}".format(ALL_UI + AMBARI))
    logger.info("Services ssl_manager can configure are: {0}".format(ALL_SERVICES + RANGER))


    # To Prepare list of installed ranger plugins
    ranger_ui_to_be_considered = list(set(ALL_UI + AMBARI).intersection(installed_services))
    ranger_services_to_be_considered = list(set(ALL_SERVICES + RANGER).intersection(installed_services))

    if disable is True:
        if service is not None:
            if service.upper() != "ALL":
                services = list(map(str.upper, service.split(',')))
                logger.info("Services passed through cli are: {0}".format(services))
                services_to_be_considered = list(
                    set(ALL_SERVICES+RANGER).intersection(installed_services).intersection(services))
            elif service.upper() == "ALL":
                logger.info("Services passed through cli are: {0}".format(service))
                services_to_be_considered = list(set(ALL_SERVICES+RANGER).intersection(installed_services))

            if is_hadoop_required(services_to_be_considered, IS_HADOOP) is True:
                services_to_be_considered.append('HADOOP')
            disable_service(services_to_be_considered, cluster, conf_file)

        if ui is not None:
            if ui.upper() != "ALL":
                uis = list(map(str.upper, ui.split(',')))
                logger.debug("UI's passed through cli are: {0}".format(uis))
                ui_to_be_considered = list(set(ALL_UI+AMBARI).intersection(installed_services).intersection(uis))
            elif ui.upper() == "ALL":
                logger.debug("UI's passed through cli are: {0}".format(ui))
                ui_to_be_considered = list(set(ALL_UI+AMBARI).intersection(installed_services))

            if is_hadoop_required(ui_to_be_considered, IS_HADOOP) is True:
                ui_to_be_considered.append('HADOOP')
            disable_ui(ui_to_be_considered, cluster, conf_file)

    elif enable is True:
        global keystorepassword
        keystorepassword = get_password(properties, "keyStorePassword").strip()
        global truststorepassword
        truststorepassword = get_password(properties, "trustStorePassword").strip()

        if ca is True:
            generate_ca(properties, host, isoverwrite)
            if scpkeyfile is not None:
                copy_certs(properties, scpkeyfile, scpusername, crtchown)
            else:
                logger.info(CA)

        if service is not None:
            if service.upper() != "ALL":
                services = list(map(str.upper, service.split(',')))
                logger.debug("Services passed through cli are: {0}".format(services))
                services_to_be_considered = list(
                    set(ALL_SERVICES+RANGER).intersection(installed_services).intersection(services))
            elif service.upper() == "ALL":
                logger.debug("Services passed through cli are: {0}".format(service))
                services_to_be_considered = list(set(ALL_SERVICES+RANGER).intersection(installed_services))

            if is_hadoop_required(services_to_be_considered, IS_HADOOP) is True:
                services_to_be_considered.append('HADOOP')

            logger.info("Services's to enable SSL are {0}".format(services_to_be_considered))
            parse_service(services_to_be_considered, accessor, cluster, conf_file)

        if ui is not None:
            if ui.upper() != "ALL":
                uis = list(map(str.upper, ui.split(',')))
                logger.debug("UI's passed through cli are: {0}".format(uis))
                ui_to_be_considered = list(set(ALL_UI+AMBARI).intersection(installed_services).intersection(uis))
            elif ui.upper() == "ALL":
                logger.debug("UI's passed through cli are: {0}".format(ui))
                ui_to_be_considered = list(set(ALL_UI+AMBARI).intersection(installed_services))

            if is_hadoop_required(ui_to_be_considered, IS_HADOOP) is True:
                ui_to_be_considered.append('HADOOP')

            logger.info("UI's to enable SSL are {0}".format(ui_to_be_considered))
            parse_ui(ui_to_be_considered, accessor, cluster, conf_file)


if __name__ == "__main__":
    try:
        import yaml
    except Exception as e:
        print("\nNeed to install PyYAML package to use yaml. E.g., yum install PyYAML")
        sys.exit(1)
    try:
        sys.exit(main())
    except (KeyboardInterrupt, EOFError):
        print("\nAborting ... Keyboard Interrupt.")
        sys.exit(1)
