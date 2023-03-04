import os
import sys


from impala.dbapi import connect
from impala.util import as_pandas


from cmlbootstrap import CMLBootstrap
# Set the setup variables needed by CMLBootstrap
HOST = os.getenv("CDSW_API_URL").split(
    ":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split(
    "/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY") 
PROJECT_NAME = os.getenv("CDSW_PROJECT")  

# Instantiate API Wrapper
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

variables=cml.get_environment_variables()
IMPALA_HOST=variables['IMPALA_HOST']
USERNAME=variables['PROJECT_OWNER']
uservariables=cml.get_user()
USERPASS=uservariables['environment']['WORKLOAD_PASSWORD']
variables
# Connect to Impala using Impyla
# Secure clusters will require additional parameters to connect to Impala.
# Recommended: Specify IMPALA_HOST as an environment variable in your project settings

IMPALA_PORT='443'
#jdbc:impala://coordinator-ClouderaEssencesHue.dw-demo-cloudera-forum-cdp-env.djki-j7ns.cloudera.site:443/default;AuthMech=3;transportMode=http;httpPath=cliservice;ssl=1;UID=cristina.sanchez;PWD=PASSWORD

conn = connect(host=IMPALA_HOST,
               port=IMPALA_PORT,
               auth_mechanism='LDAP',
               user=USERNAME,
               password=USERPASS,
               use_http_transport=True,
               http_path='/cliservice',
               use_ssl=True)
cursor = conn.cursor()

# Execute using SQL

cursor.execute("drop table if exists icebergchurn;")
cursor.execute("CREATE EXTERNAL TABLE icebergchurn STORED AS ICEBERG TBLPROPERTIES ('iceberg.table_identifier'='default.telco_iceberg')")

conn.close()