import json
import boto3
from botocore.exceptions import ClientError
import botocore
import re
import time
import boto3
import uuid
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

config = botocore.config.Config(
    read_timeout=900,
    connect_timeout=900,
    retries={"max_attempts": 1}
)


client = boto3.client("bedrock-runtime", region_name="us-west-2", config=config)

with open("examples.txt","r") as f:
    examples = f.read()

def generate_insights(hourly_csv, child_csv, group_csv, no_of_insights, insights_generation_prompt, Insights_generation_model_id, blocked_words, paraphrase_or_not):
    '''
    Takes three data files as csv strings and number of insights to generate and return a list of insights
    '''
    if not paraphrase_or_not and blocked_words:
        guardrail = None
        try:
            sensitive_words = {
                'text': [
                ]
            }

            for b_w in blocked_words:
                sensitive_words['text'].append({"text": b_w})
            client_guardrail = boto3.client('bedrock', region_name='us-west-2')
            client_token = str(uuid.uuid4())

            guardrails = client_guardrail.list_guardrails()["guardrails"]
            if "guardrail" not in [guad["name"] for guad in guardrails]:
                guardrail = client_guardrail.create_guardrail(
                name='guardrail',
                description='Guardrail for insights generated from opus model',
                wordPolicyConfig={
                    'wordsConfig': sensitive_words['text'],
                    'managedWordListsConfig': [
                        {
                            'type': 'PROFANITY'
                        },
                    ]
                },
                blockedInputMessaging='HARSH WORDS DETECTED',
                blockedOutputsMessaging='HARSH WORDS DETECTED',
                clientRequestToken=str(client_token),
                )
                print("Guardrail created successfully in generate_insights")
            else:
                guad = [guad for guad in guardrails if guad["name"] == "guardrail"][0]
                guardrail = client_guardrail.update_guardrail(
                    guardrailIdentifier=guad["id"],
                    name='guardrail',
                    description='Guardrail for insights generated from opus model',
                    wordPolicyConfig={
                        'wordsConfig': sensitive_words['text'],
                        'managedWordListsConfig': [
                            {
                                'type': 'PROFANITY'
                            },
                        ]
                    },
                    blockedInputMessaging='HARSH WORDS DETECTED',
                    blockedOutputsMessaging='HARSH WORDS DETECTED',
  
                    )
                print("Guardrail updated successfully in generate_insights")
          
                
            print("Guardrail created successfully in generate_insights")
        except Exception as e:
            logger.info(f"Error in creating guardrail in generate: {e}")
    logger.info("Generating insights")
    generate_prompt = f'''{insights_generation_prompt.replace(r"{hourly_csv}", hourly_csv).replace(r"{child_csv}", child_csv).replace(r"{group_csv}", group_csv).replace(r"{examples}",examples).replace(r"{no_of_insights}",str(no_of_insights))}'''
    native_request = {
    "anthropic_version": "bedrock-2023-05-31",
    "max_tokens": 200000,
    "temperature": 0.5,
    "messages": [
        {
            "role": "user",
            "content": [{"type": "text", "text": generate_prompt}],
        }
    ],
    }

    # Convert the native request to JSON.
    request = json.dumps(native_request)

    try:
        # Invoke the model with the request.
        if not paraphrase_or_not and blocked_words and guardrail:
            print("Guardrail exists: ", guardrail)
            print("blocked words: ", blocked_words)
            response = client.invoke_model(modelId=Insights_generation_model_id, body=request, guardrailIdentifier=guardrail["guardrailId"], guardrailVersion=guardrail["version"])
        else:
            response = client.invoke_model(modelId=Insights_generation_model_id, body=request)

        print("Response ",response)
    except (ClientError, Exception) as e:
        logger.error(f"ERROR: Can't invoke '{Insights_generation_model_id}'. Reason: {e}")
        return

    # Decode the response body.
    model_response = json.loads(response["body"].read())

    # Extract and print the response text.
    response_text = model_response["content"][0]["text"]    

    # extract the actual insights
    pattern = r"\d+\.\s(.*?)(?=\n\d+\.|\n\n|\Z)"
    matches = re.findall(pattern, response_text, re.DOTALL)
    insights = [match.strip() for match in matches]

    if response_text == "HARSH WORDS DETECTED":
        return "HARSH WORDS DETECTED"
    return insights