# export AWS_ACCESS_KEY_ID=access
# export AWS_SECRET_ACCESS_KEY=secret
# export ENDPOINT=http://localhost:8014

echo "create bucket"
aws --endpoint-url http://localhost:8014 s3api create-bucket --bucket bananas
echo "put object"
aws --endpoint-url http://localhost:8014 s3api put-object --bucket bananas --key flights_1m.parquet --body ~/Downloads/flights_1m.parquet
# echo "list buckets"
# aws --endpoint-url http://localhost:8014 s3api list-buckets
echo "list objects"
aws --endpoint-url http://localhost:8014 s3api list-objects --bucket bananas
# echo "get object"
aws --endpoint-url http://localhost:8014 s3api get-object --bucket bananas --key flights_1m.parquet ./flights_1m.parquet
aws --endpoint-url http://localhost:8014 s3api head-object --bucket bananas --key flights_1m.parquet