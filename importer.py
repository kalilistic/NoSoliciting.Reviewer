import boto3
import json
import os
import sys

TABLE_NAME = "nosol-reports"
dynamodb = boto3.client("dynamodb")


def import_json_to_dynamodb(directory: str):
    """Imports items from all JSON files in a directory into DynamoDB."""
    json_files = [f for f in os.listdir(directory) if f.endswith(".json")]

    if not json_files:
        print("No JSON files found in the specified directory.")
        return

    for file_name in json_files:
        file_path = os.path.join(directory, file_name)
        print(f"Processing file: {file_path}")

        with open(file_path, "r") as file:
            items = [json.loads(line) for line in file]

        all_successful = True
        for item in items:
            try:
                dynamodb.put_item(TableName=TABLE_NAME, Item=item["Item"])
                print(f"Successfully imported item with ID: {item['Item']['id']['S']}")
            except Exception as e:
                all_successful = False
                print(f"Failed to import item with ID: {item['Item']['id']['S']}. Error: {e}")

        if all_successful:
            try:
                os.remove(file_path)
                print(f"Deleted file: {file_path}")
            except Exception as e:
                print(f"Failed to delete file: {file_path}. Error: {e}")


if __name__ == "__main__":
    json_directory = os.path.join(os.getcwd(), "json")

    if not os.path.exists(json_directory):
        print(f"Directory not found: {json_directory}")
        sys.exit(1)

    import_json_to_dynamodb(json_directory)
