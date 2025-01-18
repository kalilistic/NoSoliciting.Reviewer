import boto3
from hashlib import sha256
from collections import defaultdict

TABLE_NAME = "nosol-reports"
dynamodb = boto3.client("dynamodb")


def fetch_all_records():
    """Fetches all records from the DynamoDB table."""
    records = []
    last_evaluated_key = None

    while True:
        params = {"TableName": TABLE_NAME}
        if last_evaluated_key:
            params["ExclusiveStartKey"] = last_evaluated_key

        response = dynamodb.scan(**params)
        records.extend(response.get("Items", []))
        last_evaluated_key = response.get("LastEvaluatedKey")

        if not last_evaluated_key:
            break

    return records


def delete_record(record_id):
    """Deletes a record from DynamoDB by ID."""
    try:
        dynamodb.delete_item(
            TableName=TABLE_NAME,
            Key={"id": {"S": record_id}}
        )
        print(f"Deleted record with ID: {record_id}")
    except Exception as e:
        print(f"Failed to delete record with ID {record_id}. Error: {e}")


def find_and_delete_duplicates():
    """Finds and deletes duplicate records based on the raw content field."""
    records = fetch_all_records()
    print(f"Fetched {len(records)} records from the table.")

    # Track unique records and duplicates
    content_hash_map = defaultdict(list)
    duplicates = []

    # Use hash of raw content to identify duplicates
    for item in records:
        record_id = item["id"]["S"]
        content_raw = item["content"]["B"]  # Raw bytes, no decoding

        # Create a hash of the raw content
        content_hash = sha256(content_raw).hexdigest()

        if content_hash in content_hash_map:
            duplicates.append(record_id)
        else:
            content_hash_map[content_hash].append(record_id)

    print(f"Found {len(duplicates)} duplicate records.")

    # Delete duplicates
    for record_id in duplicates:
        delete_record(record_id)

    remaining_records = len(content_hash_map)
    print(f"Deleted {len(duplicates)} duplicates.")
    print(f"Remaining unique records: {remaining_records}")


if __name__ == "__main__":
    find_and_delete_duplicates()
