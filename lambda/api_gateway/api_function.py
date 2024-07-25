import json
import boto3
import os
import logging


s3 = boto3.client('s3')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    try:
        logger.info("Received event: " + json.dumps(event))
        bucket_name = os.environ['S3_BUCKET_NAME']
        response = s3.list_objects_v2(Bucket=bucket_name)
        query_params = event.get('queryStringParameters')
        key_to_search = query_params.get('group_no')
        if not key_to_search:
            logger.error("No group_no provided in the query parameters.")
            return {
            'statusCode': 404,
            'body': json.dumps({"error": "No 'group_no' provided in the query parameters. please provide a group_no."})
            } 
        groups = key_to_search.split(',')
        matches_keys = []
        for g in groups:
            for obj in response.get('Contents', []):
                if g in obj['Key']:
                    matches_keys.append(obj['Key'])
        if not matches_keys:
            logger.error("No insights for requested groups found.")
            return  {
            'statusCode': 404,
            'body': json.dumps({"error": "No insights for requested groups found."})
            } 
        group_file = matches_keys[0]
        file_obj = s3.get_object(Bucket=bucket_name, Key=f"{group_file}")
        file_content = file_obj['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        json_content["Group No"] = ",".join(group_file.split("_")).replace(".json", "")
        logger.info("Successfully processed the request.")
        return {
            'statusCode': 200,
            'body': json.dumps(json_content)
        } 
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({"error": "An error occurred while processing the request."})
            } 