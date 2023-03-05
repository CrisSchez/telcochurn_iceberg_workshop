import os
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

uservariables=cml.get_user()
if uservariables['username'][-3] == '0':
  DATABASE = "u"+uservariables['username'][-3:]
else:
  #DATABASE = uservariables['username']
  DATABASE = 'u001'

#read input file
fin = open("visuals.json", "rt")
#read file contents to string
data = fin.read()
#replace all occurrences of the required string
data = data.replace('u001.telco_data_curated','default.icebergchurn')
#close the input file
fin.close()
#open the input file in write mode
fin = open("visuals.json", "wt")
#overrite the input file with the resulting data
fin.write(data)
#close the file
fin.close()
