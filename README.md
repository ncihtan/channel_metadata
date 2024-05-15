# channel_metadata

Scripts to perform curation and harmonization of channel names / antibody targets for multiplexed tissue imaging data from the Human Tumor Atlas Network

## Run on 14 May 2024:

- Found 12733 combined attributes.
- Found 1129 unique combined attributes.
- 513 antigens after LLM harmonization.

Top 10 antigens by number of HTAN data files using them:

| LLM Harmonized Antigen | entityId Count |
|------------------------|----------------|
| Nuclear                | 2257           |
| CD68                   | 2004           |
| CD45                   | 1948           |
| Ki-67                  | 1909           |
| CD20                   | 1781           |
| CD8                    | 1740           |
| CD3                    | 1690           |
| VIM                    | 1597           |
| CD4                    | 1587           |
| PD-1                   | 1473           |


## Dependencies

Ensure the following Python packages are installed:

- `boto3`
- `google-cloud-bigquery`
- `pandas`
- `tqdm`
- `concurrent.futures`

You also need to set up authentication for AWS and Google Cloud Platform (GCP):

- **AWS**: Configure your AWS credentials using the profile name `htan-dev`.
- **GCP**: Ensure your GCP credentials are set up to use BigQuery.

## Steps

### 1. Create Clients

- **AWS Bedrock Runtime Client**: Used to interact with the language model for text processing.
- **BigQuery Client**: Used to execute SQL queries and retrieve data.

### 2. Define Helper Functions

- **`curate_antigen_manual(antigen)`**: Uses regular expressions to clean antigen names by removing unwanted characters and standardizing names.
- **`parse_json_garbage(s)`**: Parses JSON strings from potentially malformed input, attempting to recover valid JSON data.
- **`initial_prompt(antigen)`**: Generates a prompt for the language model to extract and harmonize gene names from a given input string.
- **`prompt_llm(user_message, model_id, client)`**: Sends the prompt to the LLM and retrieves the response.

### 3. Load and Query Data

- **Read SQL Query**: Load the SQL query from a file.
- **Execute Query**: Retrieve data from BigQuery and convert it to a pandas DataFrame.

### 4. Process Antigen Names

- **Extract Unique Antigens**: Identify unique antigen names from the DataFrame.
- **Manual Cleaning**: Apply `curate_antigen_manual` to clean the antigen names.
- **Prepare User Prompts**: Generate prompts for each cleaned antigen name.

### 5. LLM Processing

- **Concurrent Processing**: Use a thread pool to send prompts to the LLM and parse the responses.
- **Response Handling**: Store and process the responses to create a mapping of original to harmonized antigen names.

### 6. Compile Results

- **Combine Data**: Merge original, cleaned, and harmonized antigen names into a final output table.
- **Count Table**: Create a count table to summarize the number of unique source IDs per harmonized antigen.

### 7. Save Outputs

- **Save Data**: Output the results to CSV and JSON files.

## Detailed Function Descriptions

### Manual Curation

`curate_antigen_manual(antigen)`: This function uses a series of regular expressions to clean and standardize antigen names. It removes common unwanted patterns such as numbers in brackets, prefixes like "Target:" or "Antigen", and suffixes like "-AF488". It also standardizes common variations of certain antigen names, ensuring uniformity.

### JSON Parsing

`parse_json_garbage(s)`: This function attempts to parse JSON data from a given string. If the string contains errors, it tries to parse up to the position of the error, making a best-effort attempt to recover valid JSON.

### Initial Prompt for LLM

`initial_prompt(antigen)`: This function generates a detailed prompt for the LLM to process an antigen name. The prompt instructs the LLM to extract the gene name, separate additional identifiers, and format the information into a JSON dictionary.

### LLM Interaction

`prompt_llm(user_message, model_id, client)`: This function sends the user message to the LLM and retrieves the response. The response is expected to be in JSON format, containing the harmonized gene name and additional information.

### Execution and Data Handling

- **Query Execution**: The script executes a predefined SQL query to retrieve antigen data from BigQuery.
- **Data Sampling**: It extracts unique antigen names from the dataset.
- **Manual Cleaning**: Applies the manual curation function to clean the antigen names.
- **LLM Processing**: Sends each cleaned antigen name to the LLM for further harmonization and extracts the response.

### Output

The script generates and saves the following outputs:
- **`manually_cleaned_antigens.csv`**: Contains manually cleaned antigen names.
- **`output_responses.json`**: Contains the LLM responses for each antigen.
- **`output_antigens.csv`**: Combines original, cleaned, and harmonized antigen names.
- **`output_count_table.csv`**: Summarizes the unique source IDs per harmonized antigen.

## Running the Script

1. Ensure your AWS and GCP credentials are set up.
2. Install the required Python packages.
3. Place your SQL query in a file named `query.sql`.
4. Run the script.

The script will process the data, interact with the LLM, and generate the output files.