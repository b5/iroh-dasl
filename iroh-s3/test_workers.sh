# Instructions for testing worker nodes. requires a local running docker instance

# TERMINAL 1: ("node 1")
# create a workspace:
export RUST_LOG="iroh_s3=debug"
cargo run serve

# TERMINAL 2:
export AWS_ACCESS_KEY_ID=access
export AWS_SECRET_ACCESS_KEY=secret
aws --endpoint-url http://localhost:8015 s3api create-bucket --bucket bananas
# (copy the ticket from terminal 1 log output)

# TERMINAL 3:
export RUST_LOG="iroh_s3=debug"
cargo run serve --s3-port=9014 --domain-name=localhost:9014 --data-dir="/Users/b5/Desktop/iroh_s3" --worker-port=9015 --iroh-port=9016 --join-workspace-name=bananas --join-workspace-ticket="$TICKET"

# run a job, scheduling with node 1, executing with node 2
curl -X POST -H "Content-Type: application/json" -d '{ "job_type": "Docker", "name": "test", "command": ["ls", "-l", "/"], "image": "alpine:3" }' http://localhost:8015/bananas/jobs
# observe the output of docker command in terminal 2