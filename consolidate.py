import json
import re
import boto3
import random
import pandas as pd
from tqdm import tqdm
from google.cloud import bigquery

# Create a Bedrock Runtime client in the AWS Region of your choice.
session = boto3.Session(profile_name="htan-dev")
client = session.client("bedrock-runtime", region_name="us-east-1")

# Create a BigQuery client.
bq_client = bigquery.Client()

# Set the model ID, e.g., Llama 3 8B Instruct.
model_id = "meta.llama3-70b-instruct-v1:0"


# Define helper functions
def curate_antigen_manual(antigen):
    """
    Use regular expressions to manually curate the antigen names.
    Uses a series of regex substitutions to clean up the antigen names.

    Args:
        antigen (str): The antigen name to curate.

    Returns:
        str: The manually curated antigen name.
    """
    substitutions = [
        (r"\(\d+\)$", ""),  # Remove numbers in brackets that sometimes denote cycles
        (r"^Target:", "", re.IGNORECASE),  # Remove the prefix 'Target:'
        (r"^Antigen", "", re.IGNORECASE),  # Remove the prefix 'Antigen'
        (r"nm$", ""),  # Remove the suffix nm
        (r"-AF\d+$", "", re.IGNORECASE),  # Remove suffixes like -AF488
        (r"-ArgoFlour \d+$", "", re.IGNORECASE),  # Remove suffixes like -ArgoFlour 488
        (r"^anti[\-\s]?", "", re.IGNORECASE),  # Remove the prefix 'Anti-' or 'Anti '
        (r"^CK-", "CK", re.IGNORECASE),  # Remove hyphen when prefixed with CK
        (r"CytoKRT", "cytokeratin"),  # Standardize cytokeratin
        (r"^DAPI\-?\d+$", "DAPI", re.IGNORECASE),  # Standardize DAPI
        (
            r"dna\d+",
            "DNA",
            re.IGNORECASE,
        ),  # Standardize dna followed by number denoting cycles to DNA
        (r"_\d+$", ""),  # Remove undersce number endings
        (r"\(D\)$", ""),  # Remove suffixes like (D)
        (r"[\s_-]+$", ""),  # strip trailing hyphens, underscores, and whitespace
    ]

    antigen = str(antigen)

    for pattern, repl, *flags in substitutions:
        flag = flags[0] if flags else 0
        antigen = re.sub(pattern, repl, antigen, flags=flag)

    return antigen.strip().upper()
    return antigen


def parse_json_garbage(s):
    """
    Attempt to parse a JSON string from the input string.
    If the JSON string is not valid, attempt to parse up to the position of the error.
    If no JSON start is found, return None.

    Args:
        s (str): The input string to parse.

    Returns:
        dict: The parsed JSON dictionary.
    """
    # Attempt to find the first index of '{' or '[' which likely starts the JSON object or array
    try:
        start_index = next(idx for idx, c in enumerate(s) if c in "{[")
    except StopIteration:
        # Return None or raise a more descriptive error if no JSON start is found
        return None

    # Try to parse JSON from this index
    try:
        return json.loads(s[start_index:])
    except json.JSONDecodeError as e:
        # If there's a decoding error, try to parse again up to the position of the error
        return json.loads(s[start_index : start_index + e.pos])


def initial_prompt(antigen):

    user_message = f"""
    Task: 
    
    - Extract and harmonize the gene name from a given input string, and represent 
    the information in a JSON dictionary format. 
    - The string might include additional identifiers or tags which should be separated from the main gene identifier. 
    - For Nuclear markers such as DAPI, Hoechst or DNA keep the gene name as "Nuclear".
    - For secondary antibody blanks such as "Goat Anti-Rabbit IGG", keep the gene name as "Secondary Antibody".
    - For Blank or Empty channels, keep the gene name as "Blank".
    - For Autofluorescence channels such as "Autofluorescence", keep the gene name as "Autofluorescence".
    - For Ki-67 or Ki67, keep the gene name as "Ki-67" (with the hyphen where this is standard practice).
    - For numeric or unknown gene names, or for NA/Nan values, set the gene name as "Unknown".
    - Standardise accross common names eg Vitementin, Vimentin, VIM, Vim should all be harmonized to "VIM".
    - Take care to disambiguate pan- markers and specific markers eg between pan-cytokeratin and cytokeratin 8.

    Input: Provide a string that contains a gene name, potentially mixed with additional 
    codes or identifiers (e.g., "CD206/042"). 

    Instructions:

    - Clearly identify the primary gene name from the string, disregarding any non-standard 
    identifiers or codes appended to it.

    - Output the results as a JSON dictionary with specific fields. No programming or 
    script writing is required; simply fill in the JSON structure provided below.

    The JSON dictionary should include:
    
    - original_string: The exact input string.
    - harmonized_gene_name: The standardized gene name derived from the input.
    - common_name: The widely accepted common name of the gene, if available.
    
    Expected Output:

    - Directly provide a JSON dictionary with the fields filled as specified above.
    - Do not generate Python or any other programming code to create this dictionary.
    
    Example:

    If the input is "CD206/042", you should directly provide:

    ```json
    {{
    "original_string": "CD206/042",
    "harmonized_gene_name": "CD206",
    "common_name": "Mannose receptor"
    }}
    ```

    Output Requirements:

    - Ensure the output strictly follows JSON format with the specified keys. 
    - The model should not produce any programming code or scripts, just a ready-to-use 
    JSON dictionary.

    Input antigen:

    ```
    {antigen}
    ```
    """

    return user_message
    """
    Task:

    - Consolidate gene names that are synonymous or similar into a single harmonized name and represt as a JSON dictionary.
    - The string might include additional identifiers or tags which should be separated from the main gene identifier.
    - For Nuclear markers such as DAPI, Hoechst or DNA keep the gene name as "Nuclear".
    - For secondary antibody blanks such as "Goat Anti-Rabbit IGG", keep the gene name as "Secondary Antibody".
    - For Blank or Empty channels, keep the gene name as "Blank".
    - For Autofluorescence channels such as "Autofluorescence", keep the gene name as "Autofluorescence".
    - For Ki-67 or Ki67, keep the gene name as "Ki-67" (with the hyphen where this is standard practice).
    - For numeric or unknown gene names, or for NA/Nan values, set the gene name as "Unknown".
    - Standardise accross common names eg Vitementin, Vimentin, VIM, Vim should all be harmonized to "VIM".
    - Take care to disambiguate pan- markers and specific markers eg between pan-cytokeratin and cytokeratin 8.

    Input: Provide a string that contains a gene name, potentially mixed with additional
    codes or identifiers (e.g., "[CD206, VIM, Vimentin]").

    Instructions:

    - Clearly identify the primary gene name from the string, disregarding any non-standard
    identifiers or codes appended to it.

    - Output the results as a JSON dictionary with specific fields. No programming or
    script writing is required; simply fill in the JSON structure provided below.

    The JSON dictionary should include:

    - the harmonized gene name as the key
    - a list of synonymous or similar gene names as the value

    Expected Output:

    - Directly provide a JSON dictionary with the fields filled as specified above.
    - Do not generate Python or any other programming code to create this dictionary.

    Example:

    If the input is "[CD206, VIM, Vitimentin]", you should directly provide:

    ```json
    {{
    "CD206": ["CD206"],
    "Vimentin": ["VIM", "Vimentin"],
    }}
    ```

    Output Requirements:

    - Ensure the output strictly follows JSON format with the specified keys.
    - The model should not produce any programming code or scripts, just a ready-to-use
    JSON dictionary.

    Input antigen:

    ```
    {antigen_list}
    ```
    """


def prompt_llm(user_message, model_id=model_id, client=client):
    """
    Prompt the LLM with the user message and return the generated response.

    Args:
        user_message (str): The user message to send to the LLM.
        model_id (str): The model ID to use for inference.
        client (botocore.client.Boto3): The Bedrock Runtime client.

    Returns:
        str: The generated response from the LLM.
    """

    # Embed the message in Llama 3's prompt format.
    prompt = f"""
    <|begin_of_text|>
    <|start_header_id|>user<|end_header_id|>
    {user_message}
    <|eot_id|>
    <|start_header_id|>assistant<|end_header_id|>
    """

    # Format the request payload using the model's native structure.
    request = {
        "prompt": prompt,
        # Optional inference parameters:
        "max_gen_len": 2048,
        "temperature": 0,
        "top_p": 0.9,
    }

    # print("Invoking model...")
    # Encode and send the request.
    response = client.invoke_model(body=json.dumps(request), modelId=model_id)

    # print("Decoding response...")
    # Decode the native response body.
    model_response = json.loads(response["body"].read())

    # print(f"Prompt Token count:  {model_response['prompt_token_count']}")
    # print(f"Generation Token count:  {model_response['generation_token_count']}")
    # print(f"Stop reason:{model_response['stop_reason']}")
    # Extract and print the generated text.
    response_text = model_response["generation"]

    return response_text


# read query from file
with open("query.sql", "r") as f:
    query = f.read()

# Execute the query and convert the results to a DataFrame.
print("Executing query...")
query_job = bq_client.query(query)
df = query_job.to_dataframe()

# Sample 20 rows from the dataframe
# df = df.sample(1)

print(df.shape)

# Create a list of unique antibodies, markers and channels from the df
unique_antibodies = df["Antibody_Name"].explode().unique()
unique_markers = df["Marker_Name"].explode().unique()
unique_channels = df["Channel_Name"].explode().unique()

# Combine these into a single list
unique_antigens = list(
    set(unique_antibodies) | set(unique_markers) | set(unique_channels)
)


print(f"{len(unique_antigens)} unique antigens before manual cleaning")


manually_cleaned_antigens = [
    curate_antigen_manual(antigen) for antigen in unique_antigens
]
manually_cleaned_antigens = list(set(manually_cleaned_antigens))

# Make a dictionary of the manually cleaned antigens
# Have the original antigen as the key and the cleaned antigen as the value
manually_cleaned_antigens_dict = {
    antigen: curate_antigen_manual(antigen) for antigen in unique_antigens
}

print(f"{len(manually_cleaned_antigens)} unique antigens after cleaning:")


# Save the unique antigens to a csv file with column name "Antigens"
manually_cleaned_antigens_df = pd.DataFrame(
    manually_cleaned_antigens, columns=["Antigens"]
)
manually_cleaned_antigens_df.to_csv("manually_cleaned_antigens.csv", index=False)

# For each antigen, build the user prompt, pass to the llm, and return the response as json, append to a list
response_list = []


for antigen in (pbar := tqdm(manually_cleaned_antigens)):
    pbar.set_description(str(antigen))

    user_message = initial_prompt(antigen)
    response_text = prompt_llm(user_message)
    # Extract the JSON dictionary from the response text.
    try:
        json_dict = parse_json_garbage(response_text)
        response_list.append(json_dict)
    except json.JSONDecodeError:
        json_dict = {
            "original_string": antigen,
            "response": response_text,
            "error": "The response is not a valid JSON dictionary.",
        }
        response_list.append(json_dict)
        continue


cleaned_to_harmonized = {
    item["original_string"]: item["harmonized_gene_name"]
    for item in response_list
    if "harmonized_gene_name" in item
}

# Get the number of unique values in the cleaned_to_harmonized dictionary
unique_harmonized = len(set(cleaned_to_harmonized.values()))
print(f"{unique_harmonized} antigens after LLM harmonization.")


# Combine all information into a final output table
output_data = []

for index, row in df.iterrows():
    # Create a list of unique antibodies, markers and channels from the df
    unique_antibodies = df["Antibody_Name"].explode().unique()
    unique_markers = df["Marker_Name"].explode().unique()
    unique_channels = df["Channel_Name"].explode().unique()

    # Combine these into a single list
    original_antigens = list(
        set(unique_antibodies) | set(unique_markers) | set(unique_channels)
    )

    cleaned_antigens = list(
        set(
            [
                manually_cleaned_antigens_dict.get(antigen, antigen)
                for antigen in original_antigens
            ]
        )
    )
    harmonized_antigens = list(
        set(
            [
                cleaned_to_harmonized.get(cleaned, cleaned)
                for cleaned in cleaned_antigens
            ]
        )
    )

    output_data.append(
        [
            row["Channel_Metadata_ID"],
            original_antigens,
            cleaned_antigens,
            harmonized_antigens,
            len(original_antigens),
            len(cleaned_antigens),
            len(harmonized_antigens),
        ]
    )

output_df = pd.DataFrame(
    output_data,
    columns=[
        "Source ID",
        "Original Antigens",
        "Manually Cleaned Antigens",
        "LLM Harmonized Antigens",
        "Original Antigen Count",
        "Cleaned Antigen Count",
        "Harmonized Antigen Count",
    ],
)


# Create a count table for the number of unique source IDs per consolidated antigen
count_data = (
    output_df.explode("LLM Harmonized Antigens")
    .groupby("LLM Harmonized Antigens")["Source ID"]
    .nunique()
    .reset_index()
)
count_data.columns = ["LLM Harmonized Antigen", "Unique Source ID Count"]


# Save the response list to a json file
with open("output_responses.json", "w") as f:
    json.dump(response_list, f, indent=4)

output_df.to_csv("output_antigens.csv", index=False)

count_data.to_csv("output_count_table.csv", index=False)

print("Output and count tables have been saved.")
