#!/bin/bash
# Standard Lambda deploy script — always includes psycopg2 and all dependencies.
# Pulls the psycopg2 layer from neo-admin-signals-dev (known-good), replaces only
# the handler file, then rezips and deploys. Prevents recurring missing-dependency issues.
#
# Usage:   ./deploy_lambda.sh <function-name> <local-source-path> [<entry-filename>]
# Example: ./deploy_lambda.sh neo-admin-open-positions-dev \
#            /root/Project_Neo_Damon/Lambdas/neo-admin-open-positions-dev/handler.py
# Example: ./deploy_lambda.sh neo-critical-alerter-dev \
#            /root/Project_Neo_Damon/Lambdas/neo-critical-alerter-dev/neo_critical_alerter.py \
#            neo_critical_alerter.py

set -e
FUNCTION_NAME=$1
LOCAL_SOURCE=$2
ENTRY_FILENAME=${3:-handler.py}    # defaults to handler.py; override for non-standard entry points
BUILD_DIR=/tmp/lambda_build_$$

if [ -z "$FUNCTION_NAME" ] || [ -z "$LOCAL_SOURCE" ]; then
  echo "Usage: $0 <function-name> <local-source-path> [<entry-filename>]"
  echo "  <entry-filename> defaults to handler.py — set to e.g. neo_critical_alerter.py if different"
  exit 1
fi

if [ ! -f "$LOCAL_SOURCE" ]; then
  echo "ERROR: $LOCAL_SOURCE not found"
  exit 1
fi

echo "=== Building $FUNCTION_NAME ==="
echo "    Source  : $LOCAL_SOURCE"
echo "    Entry   : $ENTRY_FILENAME"

# Pull dependency base (psycopg2 + boto3 stubs) from neo-admin-signals-dev
echo "Fetching dependency base from neo-admin-signals-dev..."
BASE_URL=$(aws lambda get-function --function-name neo-admin-signals-dev \
  --region eu-west-2 --query 'Code.Location' --output text)
curl -s -o /tmp/lambda_base_$$.zip "$BASE_URL"

# Extract, replace entry file only
mkdir -p $BUILD_DIR
cd $BUILD_DIR
unzip -q /tmp/lambda_base_$$.zip
echo "Replacing $ENTRY_FILENAME..."
cp "$LOCAL_SOURCE" "./$ENTRY_FILENAME"

# Rezip and deploy
zip -qr /tmp/${FUNCTION_NAME}_$$.zip .
echo "Deploying to AWS..."
aws lambda update-function-code \
  --function-name "$FUNCTION_NAME" \
  --zip-file "fileb:///tmp/${FUNCTION_NAME}_$$.zip" \
  --region eu-west-2 \
  --query 'FunctionArn' --output text

# Wait for update to propagate
sleep 3

# Test invoke
echo "Testing invoke..."
aws lambda invoke \
  --function-name "$FUNCTION_NAME" \
  --region eu-west-2 \
  --payload '{}' \
  /tmp/test_$$.json 2>/dev/null || true

python3 -c "
import json, sys
try:
    d = json.load(open('/tmp/test_$$.json'))
    s = str(d)[:300]
    if 'errorMessage' in s and ('psycopg2' in s or 'ModuleNotFound' in s):
        print('FAIL — dependency error:', s)
        sys.exit(1)
    elif 'errorMessage' in s:
        print('WARN — Lambda error (may be expected without auth):', s[:120])
    else:
        print('OK — Lambda responding correctly')
except Exception as e:
    print('WARN — could not parse test response:', e)
"

# Cleanup
rm -rf $BUILD_DIR /tmp/lambda_base_$$.zip /tmp/${FUNCTION_NAME}_$$.zip /tmp/test_$$.json
echo "=== Done: $FUNCTION_NAME deployed ==="
