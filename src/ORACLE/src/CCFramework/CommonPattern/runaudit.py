from google.cloud import bigquery
from airflow.utils.log.logging_mixin import LoggingMixin

class RunAudit:
    def __init__(self, name):
        self.name = name

    def insert_audit_log(self,arg1,**context):
        client = bigquery.Client()
        insert_args = {'RunAuditId': 'NULL',  'SourceName': 'NULL',  'ParentSystemId': 'NULL','SystemId': 'NULL',  'Username': 'NULL','JobName': 'NULL', 'ScheduledStartDate': 'NULL'}

        #Audit_info = []
        try:
            if arg1 is not None:
                for key, value in arg1.items():
                    if key in insert_args:
                        insert_args[key] = value

                if insert_args['ScheduledStartDate'] !='NULL':
                    scheduled_start_date = f"datetime('{insert_args['ScheduledStartDate']}')"
                else:
                    scheduled_start_date = 'NULL'
                    
                for key, value in insert_args.items():
                    LoggingMixin().log.info("{}: {}".format(key, value))
                    
        # Construct the stored procedure call for inserting audit log
                stored_procedure_insert = f"""
            CALL `dataplatr-sandbox.Ods.sp_InsertAirflowRunAuditInfo`(
                '{insert_args['RunAuditId']}',
                '{insert_args['SourceName']}',
                {insert_args['ParentSystemId']},
                {insert_args['SystemId']},
                '{insert_args['Username']}',
                '{insert_args['JobName']}',
                {scheduled_start_date}
            )
        """
                LoggingMixin().log.info("procedure call{}".format(stored_procedure_insert))
        # Execute the stored procedure call for inserting audit log
                client.query(stored_procedure_insert)
                runauditid = insert_args['RunAuditId']
                LoggingMixin().log.info("Audit ID {}".format(runauditid))
                #Audit_info.append(runauditid)
        except Exception as e:
            # Handle the exception here, you can log it or perform any necessary actions.
            LoggingMixin().log.error(f"An error occurred: {str(e)}")
        
        return runauditid

    def update_audit_info(self,arg1,**context):
        client = bigquery.Client()
        update_args= {'RunAuditId': 'NULL','LastCompletedDate': 'NULL','Status': 'NULL', 'Reason':'NULL'}
        try:
            if arg1 is not None:
                for key, value in arg1.items():
                    if key in update_args:
                        update_args[key] = value
            
                if update_args['LastCompletedDate'] != 'NULL':
                    last_completed_date = f"datetime('{update_args['LastCompletedDate']}')"
                else:
                    last_completed_date = 'NULL'

                for key, value in update_args.items():
                    LoggingMixin().log.info("{}: {}".format(key, value))
        # Construct the stored procedure call for updating run audit information
                stored_procedure_update = f"""
            CALL `dataplatr-sandbox.Ods.sp_UpdateAirflowRunAuditInfo`(
                '{update_args['RunAuditId']}',
                {last_completed_date},
                '{update_args['Status']}',
                '{update_args['Reason']}'
            )
        """
                LoggingMixin().log.info("procedure call: {}".format(stored_procedure_update))
        # Execute the stored procedure call for updating run audit information
                client.query(stored_procedure_update)

        except Exception as e:
            # Handle the exception here, you can log it or perform any necessary actions.
            LoggingMixin().log.error(f"An error occurred: {str(e)}")
                
    def get_runid_timeframe(self,**context): 
        try:
            ti = context['ti']
            result = ti.xcom_pull(task_ids='InsertRunaudit')
            #LoggingMixin().log.info("RunAuditId is {}".format(result))
            # Assuming result is a list of records
            if result:
                # Assuming the result is a list of records
                runauditid = result
                LoggingMixin().log.info("runauditid:{}".format(runauditid))
            return runauditid
        except Exception as e:
            LoggingMixin().log.error(f"An error occurred in get_runid_timeframe: {str(e)}")
            return None 