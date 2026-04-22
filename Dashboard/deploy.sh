#!/bin/bash
set -e
echo "Deploying admin dashboard..."
aws s3 cp /root/Project_Neo_Damon/Dashboard/admin-index.html s3://algodesk-admin-dashboard-dev/index.html --content-type "text/html" --cache-control "no-cache" --no-progress
echo "Deploying trading dashboard..."
aws s3 cp /root/Project_Neo_Damon/Dashboard/trading-index.html s3://algodesk-trading-dashboard-dev/index.html --content-type "text/html" --cache-control "no-cache" --no-progress
echo "Invalidating CloudFront..."
aws cloudfront create-invalidation --distribution-id E16A8HVTFGN2V0 --paths "/*" --no-cli-pager
echo "Done."
