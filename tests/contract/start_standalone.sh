#!/usr/bin/env bash
# Starts `airflow standalone` with the contract DAGs and exports the test credentials to $GITHUB_ENV.
#
# Primary path: the admin password Airflow writes to simple_auth_manager_passwords.json.generated.
# Fallback, when that file is missing: restart with all admins and take a token without a password.
set -euo pipefail

export AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow}"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER="$(cd "$(dirname "$0")" && pwd)/dags"
URL="http://localhost:8080"
PASS_FILE="$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
mkdir -p "$AIRFLOW_HOME"

start_airflow() {
  nohup airflow standalone > "$AIRFLOW_HOME/standalone.log" 2>&1 &
}

wait_for_api() {
  for _ in $(seq 1 120); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$URL/api/v2/version" || true)
    # 200 when the version is public, 401 when it needs a token: both mean the API is up.
    if [ "$code" = "200" ] || [ "$code" = "401" ]; then
      return 0
    fi
    sleep 5
  done
  echo "Airflow API did not come up; last log lines:" >&2
  tail -n 40 "$AIRFLOW_HOME/standalone.log" >&2 || true
  return 1
}

export_env() {
  {
    echo "DEPGRAPH_CONTRACT_URL=$URL"
    echo "$1"
  } >> "$GITHUB_ENV"
}

start_airflow
wait_for_api

if [ -f "$PASS_FILE" ]; then
  password=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["admin"])' "$PASS_FILE")
  echo "::add-mask::$password"
  export_env "DEPGRAPH_CONTRACT_USER=admin"
  echo "DEPGRAPH_CONTRACT_PASSWORD=$password" >> "$GITHUB_ENV"
  echo "credentials: generated admin password"
  exit 0
fi

echo "::warning::no generated password file; restarting with all admins and no password"
pkill -f "airflow standalone" || true
sleep 10
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
start_airflow
wait_for_api

token=$(curl -fsS -X POST "$URL/auth/token" -H 'Content-Type: application/json' -d '{}' \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["access_token"])')
echo "::add-mask::$token"
echo "DEPGRAPH_CONTRACT_TOKEN=$token" >> "$GITHUB_ENV"
export_env "DEPGRAPH_CONTRACT_USER="
echo "credentials: all-admins token"
