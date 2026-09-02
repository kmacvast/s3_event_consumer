#!/bin/bash
set -euo pipefail

DICTIONARY="/tmp/tech_jargon.txt"
DICT_URL="https://raw.githubusercontent.com/smoeding/hunspell-jargon/master/jargon.txt"
BUCKET_PATH="s3://kmacs-data-bucket-02/demofiles"

# 1. Error Check: Verify AWS CLI is installed
if ! command -v aws &> /dev/null; then
  echo "Error: 'aws' CLI tool is not installed." >&2
  exit 1
fi

# 2. Download the tech jargon dictionary if it doesn't exist locally
if [[ ! -f "$DICTIONARY" ]]; then
  echo "Downloading tech jargon dictionary..."
  # Downloads the file, cleans out numbers/symbols, lowercases everything, and keeps valid words
  curl -sSL "$DICT_URL" \
    | tr '[:upper:]' '[:lower:]' \
    | grep -E '^[a-z]+$' > "$DICTIONARY" || true
fi

# Verify dictionary is not empty
if [[ ! -s "$DICTIONARY" ]]; then
  echo "Error: Downloaded dictionary is empty." >&2
  exit 1
fi

for i in $(seq 1 30); do
  # Grab 4 random words separated by hyphens
  WORDS=$(shuf -n 4 "$DICTIONARY" | paste -sd '-')
  
  # Generate a random 3-digit number
  RAND_NUM=$(shuf -i 100-999 -n 1)
  
  FILENAME="${WORDS}-${RAND_NUM}.txt"
  SIZE_KB=$(shuf -i 1-1000 -n 1)

  # Echo the command explicitly before running it
  echo "+ aws s3 cp - \"${BUCKET_PATH}/${FILENAME}\""
  
  # Stream random data into AWS S3
  head -c "${SIZE_KB}K" /dev/urandom | aws s3 cp - "${BUCKET_PATH}/${FILENAME}"
done
