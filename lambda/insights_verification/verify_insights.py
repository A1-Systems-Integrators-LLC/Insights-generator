from langchain_community.chat_models import BedrockChat
from langchain.agents import initialize_agent
from langchain.agents import AgentType
import re
import boto3
import json
from botocore.exceptions import ClientError
import botocore
import time

config = botocore.config.Config(
    read_timeout=900,
    connect_timeout=900,
    retries={"max_attempts": 5}
)

client = boto3.client("bedrock-runtime", region_name="us-west-2", config=config)
haiku_mode_id = "anthropic.claude-3-haiku-20240307-v1:0"

def verify_without_python_with_agent(hourly_csv, child_csv, group_csv, insight, verification_prompt, insights_verification_model_id):

    verification_prompt = f'''{verification_prompt.replace(r'{hourly_csv}', hourly_csv).replace(r'{child_csv}', child_csv).replace(r'{group_csv}', group_csv).replace(r'{insight}', insight)}'''
    llm = BedrockChat(
    model_id=insights_verification_model_id, region_name="us-east-1", config=config
    )
    agent_chain = initialize_agent([], llm, agent=AgentType.STRUCTURED_CHAT_ZERO_SHOT_REACT_DESCRIPTION, verbose=True)

    from langchain.agents import AgentExecutor
    agent_executor = AgentExecutor(
        agent=agent_chain.agent,
        tools=[],
        verbose=True,
        max_iterations=10,
    )
    try:
        out = agent_executor.invoke({"input": verification_prompt}, handle_parsing_errors=True)
        return out["output"]

    except Exception as e:
        print(e)
        return "Failed"
    

def extract_final_results(verification):
    extract_promt = f'''
    <IMMEDIATE TASK>
    you task is to find the final conclusion for the insight whether they are correct or not? string
    already contain the answer but they are unstructured. 
    </IMMEDIATE TASK>

    <INSTRUCTIONS>
    1. *STRICTLY* you must only return one word "correct" or "incorrect".
    2. *STRICTLY* YOU MUST COME UP WITH THE CONCLUSION YOUSELF, IF THERE IS NO CONCLUSION OR CORRECT OR INCORRECT MENTIONED
    THEN YOU MUST OUTPUT "failed" word.
    3. *STRICTLY* YOU MUST RETURN "failed" IF THERE IS NO FINAL ANSWER, DO NOT COME UP WITH YOUR OWN ANSWER.
    </INSTRUCTIONS>

    <GIVEN STRING>
    {verification}
    </GIVEN STRING>
    '''
    native_request = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 200000,
        "temperature": 0.5,
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": extract_promt}],
            }
        ],
    }

    # Convert the native request to JSON.
    request = json.dumps(native_request)

    try:
        # Invoke the model with the request.
        response = client.invoke_model(modelId=haiku_mode_id, body=request)

    except (ClientError, Exception) as e:
        print(f"ERROR: Can't invoke haiku. Reason: {e}")
        exit(1)

    # Decode the response body.
    model_response = json.loads(response["body"].read())

    # Extract and print the response text.
    response_text = model_response["content"][0]["text"]

    return response_text


def verify_insights(event, context):
    insights = event["insights"]
    hourly_csv = event["hourly"]
    child_csv = event["child"]
    group_csv = event["group"]
    verification_prompt = event["verification_prompt"]
    insights_verification_model_id = event["insights_verification_model_id"]
    verification_result = []
    for insight in insights:
        print("Insight: ", insight)
        if insight == "HARSH WORDS DETECTED":
            out = "failed"
        else:
            out = verify_without_python_with_agent(hourly_csv, child_csv, group_csv,insight, verification_prompt, insights_verification_model_id)
        verification_result.append(out)
        print("LLM: ",out)
        print("=====================================")
        time.sleep(60)
    print("Verification Done")
    # with open("temp_ver_results.txt", "w") as f:
    #     for i, answer in enumerate(verification_result):
    #         f.write(f"{i+1}. {answer}\n")
    final_answers = []
    for ver in verification_result:
        if ver == "failed":
            final_answers.append("failed")
        else:
            final_answers.append(extract_final_results(ver))
    for f_ans in final_answers:
        print(f_ans)
    # write the results
    # with open("verification_results.txt", "w") as f:
    #     for i, answer in enumerate(final_answers):
    #         f.write(f"{i+1}. {answer}\n")
    return final_answers


