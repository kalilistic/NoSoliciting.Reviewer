import argparse
import base64
import binascii
import csv
from io import BytesIO

import boto3

from constants import REPLACEMENTS

TABLE_NAME = "nosol-reports"
dynamodb = boto3.client("dynamodb")


def get_text(input_data: bytes) -> str:
    """Decodes and extracts readable text from byte array."""
    # noinspection PyPep8Naming
    START = 2
    # noinspection PyPep8Naming
    END = 3
    cursor = BytesIO(input_data)
    result = bytearray()

    while cursor.tell() < len(input_data):
        byte = cursor.read(1)[0]
        if byte == START:
            cursor.read(1)  # Skip kind
            length = get_int(cursor)  # Length of the data
            cursor.read(length)  # Skip the data
            end_byte = cursor.read(1)[0]
            assert end_byte == END, "Invalid format: missing END marker"
            continue

        result.append(byte)

    return result.decode("utf-8")


def get_int(cursor: BytesIO) -> int:
    """Reads an integer from the cursor based on variable-length encoding."""
    marker = cursor.read(1)[0]
    if marker < 0xD0:
        return marker - 1

    marker = (marker + 1) & 0xF
    result = bytearray(4)

    for i in range(4):
        if marker & (1 << i):
            result[3 - i] = cursor.read(1)[0]

    return int.from_bytes(result, byteorder="little")


def do_replacements(text: str) -> str:
    """Replaces specific characters in text based on the REPLACEMENTS map."""
    for needle, replacement in REPLACEMENTS.items():
        text = text.replace(needle, replacement)
    return text


def fetch_reports(last_evaluated_key=None):
    """Fetches a batch of reports from DynamoDB."""
    params = {"TableName": TABLE_NAME}
    if last_evaluated_key:
        params["ExclusiveStartKey"] = last_evaluated_key

    response = dynamodb.scan(**params)
    return response.get("Items", []), response.get("LastEvaluatedKey")


def is_base64(data: bytes) -> bool:
    """Checks if the given data is valid Base64."""
    try:
        base64.b64decode(data)
        return True
    except (binascii.Error, ValueError):
        return False


def process_item(item):
    def decode_field(field):
        """Safely decodes a Base64 field if possible."""
        if is_base64(field):
            return base64.b64decode(field)
        return field  # Return raw if not Base64

    try:
        sender = decode_field(item["sender"]["B"])
        content = decode_field(item["content"]["B"])
    except KeyError as e:
        print(f"Missing field in item: {e}")
        return None  # Skip if fields are missing

    return {
        "sender": do_replacements(get_text(sender)),
        "content": do_replacements(get_text(content)).replace("\r\n", " ").replace("\r", " ").replace("\n", " ").strip(),
        "type": int(item["type"]["N"]),  # Keep as a number
        "reason": item["reason"]["S"],
        "suggested_classification": item["suggested_classification"]["S"],
        "id": item["id"]["S"]
    }


def display_item(item: dict):
    """Displays a processed item in a readable format."""
    print("=" * 40)
    print(f"Sender: {item['sender']}")
    print(f"Content: {item['content']}")
    print(f"Type: {item['type']}")
    print(f"Original: {item['reason']}")
    print(f"Suggested: {item['suggested_classification']}")
    print("=" * 40)


def prompt_action() -> str:
    """Prompts the user for an action."""
    actions = {
        "a": "Accept suggestion",
        "d": "Delete report",
        "k": "Keep original",
        "r": "Reclassify",
        "s": "Skip"
    }
    print("Choose an action:")
    for key, value in actions.items():
        print(f"  {key}: {value}")

    while True:
        choice = input("Enter your choice: ").strip().lower()
        if choice in actions:
            return choice
        print("Invalid choice. Please try again.")


def prompt_reclassify() -> str:
    """Prompts the user for a new classification."""
    classifications = [
        "COMMUNITY", "FC", "FLUFF", "NORMAL", "PHISH",
        "RMT_C", "RMT_G", "RP", "STATIC", "STATIC_SUB", "TRADE"
    ]
    print("Choose a new classification:")
    for idx, classification in enumerate(classifications, 1):
        print(f"  {idx}: {classification}")

    while True:
        choice = input("Enter your choice (number): ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(classifications):
            return classifications[int(choice) - 1]
        print("Invalid choice. Please try again.")


def write_to_csv(csv_writer, classification, message_type, content, file):
    """Writes a record to the CSV file and flushes the buffer."""
    csv_writer.writerow([classification, message_type, content])
    file.flush()  # Ensure immediate write to the file


def delete_report(report_id: str):
    """Deletes a report from DynamoDB."""
    try:
        dynamodb.delete_item(
            TableName=TABLE_NAME,
            Key={"id": {"S": report_id}}
        )
        print(f"Deleted report with ID: {report_id}")
    except Exception as e:
        print(f"Failed to delete report with ID: {report_id}. Error: {e}")


def review_reports(output_file: str, auto_accept: bool):
    """Main function to review and process reports."""
    last_evaluated_key = None
    batch_number = 0

    with open(output_file, mode="a", newline="") as file:
        csv_writer = csv.writer(file)

        while True:
            batch_number += 1
            items, last_evaluated_key = fetch_reports(last_evaluated_key)

            if not items and not last_evaluated_key:
                break  # Exit loop when there are no more items and no last key

            print(f"Batch {batch_number} with {len(items)} item(s) to review...")
            print(f"LastEvaluatedKey: {last_evaluated_key}")

            for item in items:
                processed = process_item(item)
                if not processed:
                    continue

                if auto_accept:
                    classification = processed["suggested_classification"]
                    message_type = processed["type"]
                    write_to_csv(csv_writer, classification, message_type, processed["content"], file)
                    delete_report(processed["id"])
                else:
                    display_item(processed)
                    action = prompt_action()
                    if action == "s":
                        continue
                    elif action == "d":
                        delete_report(processed["id"])
                    elif action in {"a", "k"}:
                        classification = processed["suggested_classification"] if action == "a" else processed["reason"]
                        message_type = processed["type"]  # Numeric type
                        write_to_csv(csv_writer, classification, message_type, processed["content"], file)
                        delete_report(processed["id"])
                    elif action == "r":
                        classification = prompt_reclassify()
                        message_type = processed["type"]  # Numeric type
                        write_to_csv(csv_writer, classification, message_type, processed["content"], file)
                        delete_report(processed["id"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Review reports and accept suggestions.")
    parser.add_argument("output_file", help="The CSV file to write output to.")
    parser.add_argument("--auto-accept", action="store_true", help="Automatically accept all suggestions.")
    args = parser.parse_args()

    review_reports(args.output_file, args.auto_accept)
