import json
import boto3
from botocore.exceptions import ClientError
import botocore
import time
import logging
import traceback
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Create a Bedrock Runtime client in the AWS Region of your choice.
config = botocore.config.Config(
    read_timeout=900,
    connect_timeout=900,
    retries={"max_attempts": 1}
)

client = boto3.client("bedrock-runtime", region_name="us-west-2", config=config)

# Set the model ID, e.g., Claude 3 Haiku.
model_id = "anthropic.claude-3-haiku-20240307-v1:0"
# Format the request payload using the model's native structure.

def paraphrase(insight, Insights_paraphrasing_prompt, Insights_paraphrasing_model_id, blocked_words, first_insight):
    if first_insight and blocked_words:
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
                description='Guardrail for xyz insights generated from opus model',
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
                print("Guardrail created successfully in paraphrase")
            else:
                guad = [guad for guad in guardrails if guad["name"] == "guardrail"][0]
                guardrail = client_guardrail.update_guardrail(
                    guardrailIdentifier=guad["id"],
                    name='guardrail',
                    description='Guardrail for xyz insights generated from opus model',
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
                print("Guardrail updated successfully in para")
          
                
            print("Guardrail created successfully in para")
        except Exception as e:
            logger.info(f"Error in creating guardrail in generate: {e}")
    else:
        client_guardrail = boto3.client('bedrock', region_name='us-west-2')
        guardrails = client_guardrail.list_guardrails()["guardrails"]
        guardrail = None
        for guad in guardrails:
            if guad["name"] == "guardrail":
                guardrail = guad
                break
        

    logger.info("In the paraphrase function")
    paraphrasing_prompt = f'''{Insights_paraphrasing_prompt.replace(r"{insight}", insight)}'''
    native_request = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 200000,
        "temperature": 0.5,
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": paraphrasing_prompt}],
            }
        ],
    }

    # Convert the native request to JSON.
    request = json.dumps(native_request)

    try:
        if blocked_words and guardrail and first_insight:
            print("Guardrail exists: in paraphrase", guardrail)
            print("blocked words: in praphrase ", blocked_words)
            response = client.invoke_model(modelId=Insights_paraphrasing_model_id, body=request, guardrailIdentifier=guardrail["guardrailId"], guardrailVersion=guardrail["version"])
        elif not first_insight and blocked_words and guardrail:
            print("Not the first insght and guardrail exists: in paraphrase", guardrail)
            response = client.invoke_model(modelId=Insights_paraphrasing_model_id, body=request, guardrailIdentifier=guardrail["id"], guardrailVersion=guardrail["version"])
        else:
            print("Not using the guardrail")
            response = client.invoke_model(modelId=Insights_paraphrasing_model_id, body=request)
    except (ClientError, Exception) as e:
        logger.error(f"An error occurred: {str(e)}")


    # Decode the response body.
    model_response = json.loads(response["body"].read())
    # Extract and print the response text.
    response_text = model_response["content"][0]["text"]
    return response_text

