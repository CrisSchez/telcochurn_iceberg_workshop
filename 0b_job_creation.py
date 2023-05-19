import os
import pandas as pd
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

import cmlapi
HOST = os.getenv("CDSW_API_URL").split(":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split("/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY") 
PROJECT_NAME = os.getenv("CDSW_PROJECT")  

# Instantiate API Wrapper
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)
project_id = cml.get_project()['public_identifier']

print(project_id)
user_details = cml.get_user({})
user_obj = {"id": user_details["id"], 
            "username": user_details["username"],
            "name": user_details["name"],
            "type": user_details["type"],
            "html_url": user_details["html_url"],
            "url": user_details["url"]
            }

#client = cmlapi.default_client()
api_url= cml.host
# No arguments are required when the default_client method is used inside a session.
variables=cml.get_environment_variables()
api_key='c37c5541c2a465d48eedadf0f90f08182d47b1edd951656cfd66576b4aed2279.f507663a491c6cb9363dca09910a63da223a155bffcdd221f279eec8708c2c7b'
api_client=cmlapi.default_client(url=api_url,cml_api_key=api_key)
api_client.list_projects()

#client = cmlapi.default_client()
api_instance=api_client


create_jobs_params = {"name": "Check Model",
          "type": "manual",
          #"arguments": str([identificador,choicemetric, threshold,target_columns,drop_input_columns]),
          #"arguments": identificador,
          "project_id": project_id,
          "runtime_identifier": "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-workbench-python3.7-standard:2022.11.2-b2",
          "script": "6_check_model.py",
          "timezone": "Europe/Madrid",
          "runtime_addon_identifiers": ['spark320-18-hf3'],
          "kernel": "python3",
          "cpu" : 2,
          "memory" : 4,
          "recipients": [
                          {"email": user_details["email"],
                           "success":True,"notify_on_success": False, "notify_on_failure": False, "notify_on_timeout": False, "notify_on_stop": False
                           }
          ]
          }
api_instance.create_job(create_jobs_params, project_id)

create_jobs_params = {"name": "avisoPerformance",
          "type": "manual",
          #"arguments": str([identificador,choicemetric, threshold,target_columns,drop_input_columns]),
          #"arguments": identificador,
          "project_id": project_id,
          "runtime_identifier": "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-workbench-python3.7-standard:2022.11.2-b2",
          "script": "7_crearReportes.py",
          "timezone": "Europe/Madrid",
          "runtime_addon_identifiers": ['spark320-18-hf3'],
          "kernel": "python3",
          "cpu" : 2,
          "memory" : 4,
          "recipients": [
                          {"email": user_details["email"],
                           "success":True,"notify_on_success": False, "notify_on_failure": False, "notify_on_timeout": False, "notify_on_stop": False
                           }
          ]
          }
api_instance.create_job(create_jobs_params, project_id)
          
          
create_jobs_params = {"name": "retrain",
          "type": "manual",
          #"arguments": str([identificador,choicemetric, threshold,target_columns,drop_input_columns]),
          #"arguments": identificador,
          "project_id": project_id,
          "runtime_identifier": "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-workbench-python3.7-standard:2022.11.2-b2",
          "script": "3_trainStrategy_job.py",
          "timezone": "Europe/Madrid",
          "runtime_addon_identifiers": ['spark320-18-hf3'],
          "kernel": "python3",
          "cpu" : 2,
          "memory" : 4,
          "recipients": [
                          {"email": user_details["email"],
                           "success":True,"notify_on_success": False, "notify_on_failure": False, "notify_on_timeout": False, "notify_on_stop": False
                           }
          ]
          }
api_instance.create_job(create_jobs_params, project_id)
params = {"projectId":project_id,"latestModelDeployment":True,"latestModelBuild":True}
jobsInfo=pd.DataFrame(cml.get_jobs(params))
job_id = jobsInfo.loc[jobsInfo['name'] == 'retrain']['public_identifier'].min()
          
create_jobs_params = {"name": "deploy_best_model",
          "type": "dependent",
          "parent_job_id":job_id,
          #"arguments": str([identificador,choicemetric, threshold,target_columns,drop_input_columns]),
          #"arguments": identificador,
          "project_id": project_id,
          "runtime_identifier": "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-workbench-python3.7-standard:2022.11.2-b2",
          "script": "4_get_champion.py",
          "timezone": "Europe/Madrid",
          "runtime_addon_identifiers": ['spark320-18-hf3'],
          "kernel": "python3",
          "cpu" : 2,
          "memory" : 4,
          "recipients": [
                          {"email": user_details["email"],
                           "success":True,"notify_on_success": False, "notify_on_failure": False, "notify_on_timeout": False, "notify_on_stop": False
                           }
          ]
          }
api_instance.create_job(create_jobs_params, project_id)
