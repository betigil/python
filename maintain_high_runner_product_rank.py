import sys
import os
from pathlib import Path
if getattr(sys, 'frozen', False):
    application_path = os.path.dirname(sys.executable)
elif __file__:
    application_path = sys.path[0]
    
def get_common_package_path(path_name):
    while not ([x[0] for x in os.walk(path_name) if x[0].endswith("common_package")] or str(path_name) == str((path_name).anchor)):
        return get_common_package_path(path_name.parent)
    if str(path_name) == str((path_name).anchor):
        if [x[0] for x in os.walk(path_name) if x[0].endswith("common_package")]:
            return str([x[0] for x in os.walk(path_name) if x[0].endswith("common_package")][0])
        else:
            return "Reached till Root of File System Common_package not found buddy"
    else:
        return str([x[0] for x in os.walk(path_name) if x[0].endswith("common_package")][0])
common_package_path = get_common_package_path(Path(application_path))
print(common_package_path)
sys.path.extend([f"{common_package_path}/database",f"{common_package_path}/core"])

import pandas as pd 
import yaml
import warnings
import os
import re
import datetime as dt
import sql_server_data_module_prd as db
import logger_util as lu
import send_email as se
import send_slackalert as sa

############################################################################################################
# CD C:\Users\Public\Downloads\SisenseAdminfiles\Python\high_runner_product_rank
# python maintain_high_runner_product_rank.py "C:\Users\Public\Downloads\SisenseAdminfiles\Python\config.yml"
#
# To load the high_runner_product_rank file 
# Created by         Betigil            8/6/2024
#
#############################################################################################################

def main():
    warnings.filterwarnings("ignore")
    logger = lu.get_logger(__name__)
    errors_list = []


    with open(sys.argv[1],'r') as ymlfile:
        config_file = yaml.safe_load(ymlfile)

    print("sys.argv[1]",sys.argv[1])

    file_name = config_file['infiles']['high_runner_product_rank']

    try
        result_df = pd.read_csv(file_name)

        stored_proc = 'prc_maintain_high_runner_product_rank'

        query = "truncate table forecast.high_runner_product_rank_staging "

        db.commit_query_dml_prd(config_file,query)

        db.save_commits_prd(result_df,config_file, table_name="high_runner_product_rank_staging",schema_name = "forecast")
        print("Data loaded to staging table")
        result = db.execute_stored_procedure_prd(config_file, schema ="forecast",storedprocedure_name = stored_proc)
        print("result", result)
        print("Stored proc completed and data loaded to final table")
            
    except(Exception, TypeError) as e:
        errors_list.append(str(e))
        logger.error(str(e))
        
    if errors_list:
        errors_list = str(errors_list)
        message =   " maintain_high_runner_product_rank has failed- .\r\n\r\n File name (s): " + file_name + "\r\n\r\n " + " Error=" + str(errors_list)
        message = message + "\r\n\r\n " + "Folder:-" + file_name
        print(file_name + " has failed.\r\n\r\n Filename : " + file_name + "\r\n\r\n " + " Error=" + str(errors_list))
        subject = "Alert! There is an issue processing high_runner_product_rank" 
        se.send_email(config_file, message, subject, to_address=False)
        sa.slack_alert(subject,message,config_file['slack']['slack_channel'])

if __name__ == '__main__':
    main()
