set -e

echo "Deploying Cortex Framework for Oracle."

log_bucket=$1

cloud_build_project=$(cat "config/config.json" | python3 -c "import json,sys; print(str(json.load(sys.stdin)['projectId']))" 2>/dev/null || echo "")
if [[ "${cloud_build_project}" == "" ]]
then
    echo "ERROR: Cortex Framework for Oracle is not configured."
    exit 1
fi
echo "Using Cloud Build in project '${cloud_build_project}'"

# Read target bucket from config.json
target_bucket=$(cat "config/config.json" | python3 -c "import json,sys; print(str(json.load(sys.stdin)['targetBucket']))" 2>/dev/null || echo "")
if [[ "${target_bucket}" == "" ]]
then
    echo "ERROR: Target bucket not found in config.json."
    exit 1
fi
echo "Using target bucket '${target_bucket}' for logs."

# Setting up default logs bucket or using provided log_bucket
if [[ "${log_bucket}" == "" ]]
then
    _GCS_BUCKET="${target_bucket}"
else
    _GCS_BUCKET="${log_bucket}"
fi
echo "Using logs bucket ${_GCS_BUCKET}"

set +e
echo -e "\n\033[0;32m\033[1mPlease wait while Data Foundation is being deployed...\033[0m\n"
gcloud builds submit --config=cloudbuild.yaml --suppress-logs \
    --project "${cloud_build_project}" \
    --substitutions=_GCS_BUCKET="${_GCS_BUCKET}" . \
    && _SUCCESS="true"
if [[ "${_SUCCESS}" != "true" ]]; then
    echo -e "\n🛑 Data Foundation deployment has failed. 🛑"
    exit 1
else
    echo -e "\n✅ Data Foundation has been successfully deployed. 🦄"
fi