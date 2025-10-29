#!/bin/bash

# --- findbug.sh ---
#
# Function: Scan the logs of the specified database type and detect potential bugs
#           caused by result set mismatches.
#
# Usage: ./findbug.sh <database_type>
# Example: ./findbug.sh mysql

# --- 1. Argument Validation ---
# Check if the user has provided the database type as an argument (e.g., mysql)
if [ -z "$1" ]; then
  echo "Error: No database type provided."
  echo "Usage: $0 <database_type>"
  echo "Example: $0 mysql"
  exit 1
fi

# --- 2. Variable Definitions ---
# Get the database type from the first command-line argument
DB_TYPE="$1"

# Construct the full path to the log directory based on the database type
LOG_DIR="target/logs/$DB_TYPE"

# Construct the output file name based on the database type
OUTPUT_FILE="${DB_TYPE}bug.txt"

# Define the target string to search for
SEARCH_STRING="the result sets mismatch"

# --- 3. Environment Check and Initialization ---
# Verify that the log directory exists
if [ ! -d "$LOG_DIR" ]; then
  echo "Error: Log directory does not exist: $LOG_DIR"
  exit 1
fi

# Clear or create the output file to ensure a fresh start each time
> "$OUTPUT_FILE"

echo "Scanning directory: $LOG_DIR"
echo "Searching for logs containing '$SEARCH_STRING'..."
echo "-----------------------------------------"

# --- 4. Core Logic ---
# Enable nullglob so that no-match patterns expand to nothing instead of literal strings
shopt -s nullglob

# Iterate over all log files matching the pattern database*.log
for log_file in "$LOG_DIR"/database*.log; do
  
  # Read the first line of the current log file
  first_line=$(head -n 1 "$log_file")

  # Check if the first line contains the target string
  if [[ $first_line == *"$SEARCH_STRING"* ]]; then
    echo "Potential Bug Found: $log_file"
    # Append the log file path to the output file
    echo "$log_file" >> "$OUTPUT_FILE"
  fi
done

# Restore default shell behavior
shopt -u nullglob

# --- 5. Summary ---
# Check if the output file exists and is non-empty
if [ -s "$OUTPUT_FILE" ]; then
  BUG_COUNT=$(wc -l < "$OUTPUT_FILE")
  echo "-----------------------------------------"
  echo "Scan completed! Found $BUG_COUNT potential bugs."
  echo "List of affected log files saved to: $OUTPUT_FILE"
else
  echo "-----------------------------------------"
  echo "Scan completed! No potential bugs found."
  rm "$OUTPUT_FILE"
fi

exit 0
