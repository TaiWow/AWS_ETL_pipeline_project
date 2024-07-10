from redshift_db_connection import *
import os
from load import perform_etl

# Name of the environment variable which has the SSM parameter name as its value.
# The SSM parameter name will be "<team name>_redshift_settings".
ssm_env_var_name = 'nubi_redshift_settings'

def lambda_handler(event, context):
    print('lambda_handler: starting')

    try:

        nubi_redshift_settings = os.environ[ssm_env_var_name] or 'NOT_SET'
        print(f'lambda_handler: nubi_redshift_settings={nubi_redshift_settings} from ssm_env_var_name={ssm_env_var_name}')

        # connection
        redshift_details = get_ssm_param(nubi_redshift_settings)
        conn, cur = open_sql_database_connection_and_cursor(redshift_details)
        
        #call ETL
        perform_etl()

        print(f'lambda_handler: done')

    except Exception as whoopsy:
        # ...exception reporting
        print(f'lambda_handler: failure, error=${whoopsy}')
        raise whoopsy
    
    finally:
        # Ensure cursor and connection are closed
        if cur:
            cur.close()
        if conn:
            conn.close()