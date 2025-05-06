import boto3

def lambda_handler(event, context):
    glue = boto3.client('glue')

    # Get the file name from the event
    file_name = event['Records'][0]['s3']['object']['key']

    # Trigger specific jobs based on the file name
    if file_name == 'exported_nyc_traffic_data.csv':
        glue.start_job_run(JobName='Join_NYC_Fire_Incident_Traffic_Data_ETL_Job')
        glue.start_job_run(JobName='NYC_Traffic_Data_ETL_Job')
    elif file_name == 'exported_nyc_fire_incidents_data.csv':
        glue.start_job_run(JobName='NYC_Fire_Traffic_ETL_Job')
    else:
        return {
            'statusCode': 200,
            'body': f"No Glue job triggered for file: {file_name}"
        }

    return {
        'statusCode': 200,
        'body': f"Glue job triggered for file: {file_name}"
    }
#
