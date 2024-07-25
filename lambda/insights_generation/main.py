import pandas as pd
import numpy as np
from dateutil import parser
from generate_insights import generate_insights
from paraphrase_insights import paraphrase
import boto3
import json
import os
import zipfile
import tempfile
import datetime
from botocore.config import Config
import traceback
import logging

s3 = boto3.client('s3')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def preprocess_data(hourly, child, group):

    logger.info("In the preprocess function")

    # remove any extra spaces in the column names
    hourly.columns = [col.strip() for col in hourly.columns]
    child.columns = [col.strip() for col in child.columns]
    group.columns = [col.strip() for col in group.columns]

    # strip the values in each cell to remove any extra spaces
    for col in hourly.columns:
        if hourly[col].dtype == "object":
            hourly[col] = hourly[col].str.strip()

    for col in child.columns:
        if child[col].dtype == "object":
            child[col] = child[col].str.strip()

    for col in group.columns:
        if group[col].dtype == "object":
            group[col] = group[col].str.strip()

    # check for any empty strings
    # replace empty strings with NaN
    hourly.replace("", np.nan, inplace=True)
    child.replace("", np.nan, inplace=True)
    group.replace("", np.nan, inplace=True)

    # drop the rows with nan values
    hourly.dropna(inplace=True)
    child.dropna(inplace=True)
    group.dropna(inplace=True)

    try:
        # check for date values and convert them to consistent format
        hourly["Day Date"] = hourly["Day Date"].apply(date_format)
        child["Day Date"] = child["Day Date"].apply(date_format)
        group["Day Date"] = group["Day Date"].apply(date_format)
    except Exception as e:
        logger.error(f"An error occurred while converting date values: {str(e)}. The model might not provide the correct insights.")
        logger.debug(traceback.format_exc())
    
    prefix = ""
    if len(group["Group ID"].unique()) != 1:
        prefix = "_".join([str(i) for i in group["Group ID"].unique()])
    else:
        prefix = str(list(group["Group ID"].unique())[0])

    # convert the dataframes to the csv
    hourly_csv = hourly.to_csv(index=False)
    child_csv = hourly.to_csv(index=False)
    group_csv = group.to_csv(index=False)

    return hourly_csv, child_csv, group_csv, prefix



def date_format(date_string):
    dt_object = parser.parse(date_string)
    new_date_string = dt_object.strftime("%-m/%-d/%Y")
    return new_date_string

def main(event, context):
    try:
        # Get bucket name and key from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        zip_file_key = event['Records'][0]['s3']['object']['key']
        print("Bucket name: ", bucket_name)
        print("Zip file key: ", zip_file_key)
        if not zip_file_key.endswith('.zip'):
            logger.error("Uploaded file is not a ZIP f`ile")
            return 
        
        # Download the ZIP file to a temporary location
        with tempfile.TemporaryDirectory() as tmpdirname:
            zip_file_path = os.path.join(tmpdirname, 'uploaded.zip')
            s3.download_file(bucket_name, zip_file_key, zip_file_path)
            
            # Unzip the file
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(tmpdirname)
                
            # Log the contents of the temporary directory
            extracted_files = os.listdir(tmpdirname)
            logger.info(f"Extracted files: {extracted_files}")
            # Look for the specific CSV files
            
            hourly_csv_path = os.path.join(tmpdirname, 'file_1.csv')
            day_child_csv_path = os.path.join(tmpdirname, 'file_2.csv')
            group_csv_path = os.path.join(tmpdirname, 'file_3.csv')
            configuration = os.path.join(tmpdirname, 'config.json')
            
            requires_conf_keys = ['Insights_generation_model_id', 'Insights_paraphrasing_model_id', 'insights_verification_model_id', 'number_of_insights']
            if os.path.exists(configuration):
                with open(configuration) as f:
                    user_config = json.load(f)
            else:
                user_config = None
            blocked_words = None
            if user_config and all(key in user_config for key in requires_conf_keys):
                Insights_generation_model_id = user_config['Insights_generation_model_id']
                Insights_paraphrasing_model_id = user_config['Insights_paraphrasing_model_id']
                insights_verification_model_id = user_config['insights_verification_model_id']
                number_of_insights = user_config['number_of_insights']
                paraphrase_or_not = user_config["paraphrase_or_not"]
                if "blocked_words" in user_config:
                    blocked_words = user_config['blocked_words']
                    print("blocked words in the main: ",blocked_words)
                    # read the words
                    with open("insights_generation_prompt.txt","r") as f:
                        Insights_generation_prompt_words = f.read().split()
                        # convert to lower case
                        Insights_generation_prompt_words = [word.lower() for word in Insights_generation_prompt_words]

                    with open("insights_paraphrasing_prompt.txt","r") as f:
                        Insights_paraphrasing_prompt_words = f.read().split()
                        Insights_paraphrasing_prompt_words = [word.lower() for word in Insights_paraphrasing_prompt_words]

                    with open("insights_verification_prompt.txt","r") as f:
                        insights_verification_prompt_words = f.read().split()
                        insights_verification_prompt_words = [word.lower() for word in insights_verification_prompt_words]
                    # delete the words which are present in three prompts from the blocked words
                    blocked_words = [word for word in blocked_words if word not in Insights_generation_prompt_words and word not in Insights_paraphrasing_prompt_words and word not in insights_verification_prompt_words]
        
            else:

                Insights_generation_model_id = "anthropic.claude-3-opus-20240229-v1:0"
                Insights_paraphrasing_model_id = "anthropic.claude-3-haiku-20240307-v1:0"
                insights_verification_model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"
                number_of_insights = 5
                paraphrase_or_not = True
            with open("insights_generation_prompt.txt","r") as f:
                Insights_generation_prompt = f.read()
            with open("insights_paraphrasing_prompt.txt","r") as f:
                Insights_paraphrasing_prompt = f.read()
            with open("insights_verification_prompt.txt","r") as f:
                insights_verification_prompt = f.read()
            if not (os.path.exists(hourly_csv_path) and os.path.exists(day_child_csv_path) and os.path.exists(group_csv_path)):
                logger.error("One or more expected files are missing in the ZIP archive")
                return
    
            hourly = pd.read_csv(hourly_csv_path)
            child = pd.read_csv(day_child_csv_path)
            group = pd.read_csv(group_csv_path)
            
            # Preprocess the data 
            hourly_csv, child_csv, group_csv, prefix = preprocess_data(hourly, child, group)
            insights = generate_insights(hourly_csv, child_csv, group_csv, number_of_insights, Insights_generation_prompt,Insights_generation_model_id, blocked_words, paraphrase_or_not)
            
            if type(insights) != list or len(insights) == 0:
                timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
                bucket_name = os.environ['S3_BUCKET_NAME']
                response = s3.list_objects_v2(Bucket=bucket_name)
                group = prefix
                matches_keys = []
                for obj in response['Contents']:
                    if group in obj['Key']:
                        matches_keys.append(obj['Key'])
                if len(matches_keys) != 0:
                    data_ele = 0
                    group_file = matches_keys[0]
                    file_obj = s3.get_object(Bucket=bucket_name, Key=f"{group_file}")
                    file_content = file_obj['Body'].read().decode('utf-8')
                    json_content = json.loads(file_content)
                    if len(json_content) > 0:
                        data_ele = len(json_content)+1
                    else:
                        data_ele = 1
                    json_content[f"data_{data_ele}"] = {"error message": "SORRY! NO INSIGHTS GENERATED. TRY AGAIN","Insight_generation_timestamp":timestamp}
                    with open(f"/tmp/{prefix}.json", 'w') as f:
                        json.dump(json_content, f)
                else:
                    json_final = {f"data_1": {"error message": "SORRY! NO INSIGHTS GENERATED. TRY AGAIN","Insight_generation_timestamp":timestamp}}
                    with open(f"/tmp/{prefix}.json", 'w') as f:
                        json.dump(json_final, f)
                
                # upload the final result to s3
                s3.upload_file(f"/tmp/{prefix}.json", bucket_name, f"{prefix}.json")
                logger.info("S3 upload done!")
                return 
            
            if paraphrase_or_not:

                logger.info("Paraphrasing insights...")
                paraphrased_insights = []
                first_insight = True
                for ins in insights:
                    para = paraphrase(ins, Insights_paraphrasing_prompt, Insights_paraphrasing_model_id, blocked_words, first_insight)
                    paraphrased_insights.append(para)
                    first_insight = False
    

                
                json_insights = {"insights":[],"hourly":hourly_csv, "child":child_csv, "group":group_csv,"verification_prompt":insights_verification_prompt,"insights_verification_model_id":insights_verification_model_id}
                for ins in paraphrased_insights:
                    json_insights["insights"].append(ins)
            else:
                json_insights = {"insights":insights,"hourly":hourly_csv, "child":child_csv, "group":group_csv,"verification_prompt":insights_verification_prompt,"insights_verification_model_id":insights_verification_model_id}
            config = Config(read_timeout=900, retries={'max_attempts': 2})
            lambda_client = boto3.client('lambda', config=config)
            verification_lambda_function_name = os.environ['InsightsVerificationLambdaFunctionName']

            logger.info("Invoking the verification lambda function")
            response = lambda_client.invoke(
            FunctionName=verification_lambda_function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(json_insights)
            )

            verification_result = json.loads(response['Payload'].read().decode('utf-8'))
            final_insights = []

            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            final_result_json = {"insights":json_insights["insights"], "verification_result":verification_result, "Insight_generation_timestamp":timestamp} 

            
            file_name = f"{prefix}"

            bucket_name = os.environ['S3_BUCKET_NAME']
            response = s3.list_objects_v2(Bucket=bucket_name)
            group = prefix
            matches_keys = []
            for obj in response['Contents']:
                if group in obj['Key']:
                    matches_keys.append(obj['Key'])
            if len(matches_keys) != 0:
                data_ele = 0
                group_file = matches_keys[0]
                file_obj = s3.get_object(Bucket=bucket_name, Key=f"{group_file}")
                file_content = file_obj['Body'].read().decode('utf-8')
                json_content = json.loads(file_content)
                if len(json_content) > 0:
                    data_ele = len(json_content)+1
                else:
                    data_ele = 1
                json_content[f"data_{data_ele}"] = final_result_json
                with open(f"/tmp/{file_name}.json", 'w') as f:
                    json.dump(json_content, f)
            else:
                json_final = {}
                json_final[f"data_1"] = final_result_json
                with open(f"/tmp/{file_name}.json", 'w') as f:
                    json.dump(json_final, f)
            
            # upload the final result to s3
            s3.upload_file(f"/tmp/{file_name}.json", bucket_name, f"{file_name}.json")
            logger.info("S3 upload done!")
            return final_insights

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")


