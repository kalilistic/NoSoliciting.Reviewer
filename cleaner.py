import csv
from thefuzz import fuzz


def deduplicate_csv(input_file: str, output_file: str):
    """
    Deduplicates a CSV file based on 99% similarity in the third column,
    removes rows with missing third column, and filters out rows where
    more than 10% of the characters in the third column are Chinese or Japanese.
    """
    def is_cjk(character):
        """Check if a character is Chinese, Japanese, or Korean."""
        return any([
            "\u4e00" <= character <= "\u9fff",  # CJK Unified Ideographs
            "\u3040" <= character <= "\u309f",  # Hiragana
            "\u30a0" <= character <= "\u30ff",  # Katakana
            "\uff00" <= character <= "\uffef"   # Full-width characters
        ])

    def cjk_ratio(text):
        """Calculate the ratio of CJK characters in the text."""
        if not text:
            return 0
        cjk_count = sum(1 for char in text if is_cjk(char))
        return cjk_count / len(text)

    seen = []
    unique_rows = []

    with open(input_file, mode="r", newline="", encoding="utf-8") as infile:
        reader = csv.reader(infile)
        header = next(reader)  # Assuming the first row is the header
        unique_rows.append(header)

        for row in reader:
            if len(row) < 3 or not row[2].strip():
                # Skip rows with missing or empty third column
                continue

            if cjk_ratio(row[2]) > 0.1:
                # Skip rows where more than 10% of the text is CJK characters
                continue

            is_duplicate = False
            for seen_row in seen:
                # Check for 99% similarity with existing rows
                if fuzz.ratio(row[2], seen_row[2]) >= 99:
                    is_duplicate = True
                    break

            if not is_duplicate:
                seen.append(row)
                unique_rows.append(row)

    # Write the deduplicated rows to a new file
    with open(output_file, mode="w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerows(unique_rows)

    print(f"Deduplication complete. Output written to {output_file}")


if __name__ == "__main__":
    # Input and output file paths
    input_csv = "output.csv"  # Replace with the path to your input CSV file
    output_csv = "output2.csv"  # Replace with the path to your output CSV file

    deduplicate_csv(input_csv, output_csv)
