#!/bin/bash
# =============================================================================
# collect_dynamodb_sizing.sh
# Coleta informações completas de sizing de tabelas Amazon DynamoDB
# para auxiliar na migração / dimensionamento para MongoDB Atlas
#
# Coleta por tabela:
#   - Metadata: modo de cobrança, classe, TTL, streams, PITR, replication
#   - Capacidade: RCU/WCU provisionados, Auto Scaling policies
#   - Tamanho: item count, table size, avg item size estimado
#   - Índices: GSI e LSI com projeção, chaves, capacidade e tamanho
#   - CloudWatch (60 dias): ConsumedReadCapacity, ConsumedWriteCapacity,
#     SuccessfulRequestLatency, ThrottledRequests, SystemErrors,
#     TransactionConflict, ReturnedItemCount, StorageByTableName
#
# Saídas:
#   <table>.json            — dados brutos por tabela
#   sizing_report.json      — consolidado de todas as tabelas
#   sizing_summary.csv      — uma linha por tabela
#   indexes_detail.csv      — uma linha por índice (GSI + LSI)
#   cloudwatch_metrics.csv  — série temporal de métricas por tabela
#
# Requisitos: aws cli v2, jq
# Paralelismo: MAX_PARALLEL (default 8) tabelas simultâneas
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# CONFIGURAÇÃO
# ---------------------------------------------------------------------------
AWS_REGION="${AWS_REGION:-us-east-1}"
TABLE_PREFIX="${TABLE_PREFIX:-}"
EXACT_ITEM_COUNT="${EXACT_ITEM_COUNT:-false}"
MAX_PARALLEL="${MAX_PARALLEL:-8}"
OUTPUT_DIR="./dynamo_sizing_$(date +%Y%m%d_%H%M%S)"
REPORT_FILE="$OUTPUT_DIR/sizing_report.json"
SUMMARY_CSV="$OUTPUT_DIR/sizing_summary.csv"
INDEX_CSV="$OUTPUT_DIR/indexes_detail.csv"
CW_CSV="$OUTPUT_DIR/cloudwatch_metrics.csv"
TMP_DIR="$OUTPUT_DIR/.tmp"

# Janela CloudWatch: 60 dias, granularidade 1 dia (86400s)
CW_DAYS=60
CW_PERIOD=86400
END_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# macOS vs Linux para cálculo de data
if date -v-${CW_DAYS}d +%Y-%m-%dT%H:%M:%SZ >/dev/null 2>&1; then
  START_TIME=$(date -u -v-${CW_DAYS}d +%Y-%m-%dT%H:%M:%SZ)
else
  START_TIME=$(date -u -d "${CW_DAYS} days ago" +%Y-%m-%dT%H:%M:%SZ)
fi

mkdir -p "$OUTPUT_DIR" "$TMP_DIR"

# ---------------------------------------------------------------------------
# FUNÇÕES UTILITÁRIAS
# ---------------------------------------------------------------------------
log()  { echo "[$(date +%H:%M:%S)] $*"; }
die()  { echo "ERRO: $*" >&2; exit 1; }
hr()   { echo "────────────────────────────────────────────────────────────"; }

command -v aws >/dev/null 2>&1 || die "aws cli não encontrado"
command -v jq  >/dev/null 2>&1 || die "jq não encontrado"

num_or_zero() {
  local value="${1:-0}"
  if [[ -z "$value" || "$value" == "None" || "$value" == "null" ]]; then
    echo "0"
  elif [[ "$value" =~ ^-?[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$ ]]; then
    echo "$value"
  else
    echo "0"
  fi
}

# ---------------------------------------------------------------------------
# Coleta uma métrica CloudWatch — cada chamada usa seu próprio arquivo de erro
# ---------------------------------------------------------------------------
get_cw_metric() {
  local metric="$1" stat="$2" dims="$3" unit="${4:-}"
  local value err_file
  err_file=$(mktemp "${TMPDIR:-/tmp}/dynamo-cw-err.XXXXXX")
  # shellcheck disable=SC2064
  trap "rm -f '$err_file'" RETURN

  if [[ -n "$unit" ]]; then
    value=$(aws cloudwatch get-metric-statistics \
      --region "$AWS_REGION" \
      --namespace "AWS/DynamoDB" \
      --metric-name "$metric" \
      --dimensions "$dims" \
      --start-time "$START_TIME" \
      --end-time   "$END_TIME" \
      --period     "$CW_PERIOD" \
      --statistics "$stat" \
      --unit "$unit" \
      --query "sort_by(Datapoints,&Timestamp) | [-1].${stat}" \
      --output text 2>"$err_file") || { echo "0"; return; }
  else
    value=$(aws cloudwatch get-metric-statistics \
      --region "$AWS_REGION" \
      --namespace "AWS/DynamoDB" \
      --metric-name "$metric" \
      --dimensions "$dims" \
      --start-time "$START_TIME" \
      --end-time   "$END_TIME" \
      --period     "$CW_PERIOD" \
      --statistics "$stat" \
      --query "sort_by(Datapoints,&Timestamp) | [-1].${stat}" \
      --output text 2>"$err_file") || { echo "0"; return; }
  fi
  num_or_zero "$value"
}

# Soma todos os datapoints
get_cw_sum_all() {
  local metric="$1" stat="$2" dims="$3"
  local values err_file
  err_file=$(mktemp "${TMPDIR:-/tmp}/dynamo-cw-err.XXXXXX")
  # shellcheck disable=SC2064
  trap "rm -f '$err_file'" RETURN

  values=$(aws cloudwatch get-metric-statistics \
    --region "$AWS_REGION" \
    --namespace "AWS/DynamoDB" \
    --metric-name "$metric" \
    --dimensions "$dims" \
    --start-time "$START_TIME" \
    --end-time   "$END_TIME" \
    --period     "$CW_PERIOD" \
    --statistics "$stat" \
    --query "Datapoints[*].${stat}" \
    --output json 2>"$err_file") || { echo "0"; return; }

  echo "$values" | jq '[.[] // 0] | add // 0'
}

# Série temporal diária
get_cw_timeseries() {
  local metric="$1" stat="$2" dims="$3" table_name="$4"
  local values err_file
  err_file=$(mktemp "${TMPDIR:-/tmp}/dynamo-cw-err.XXXXXX")
  # shellcheck disable=SC2064
  trap "rm -f '$err_file'" RETURN

  values=$(aws cloudwatch get-metric-statistics \
    --region "$AWS_REGION" \
    --namespace "AWS/DynamoDB" \
    --metric-name "$metric" \
    --dimensions "$dims" \
    --start-time "$START_TIME" \
    --end-time   "$END_TIME" \
    --period     "$CW_PERIOD" \
    --statistics "$stat" \
    --query "sort_by(Datapoints,&Timestamp)[*].{ts:Timestamp,val:${stat}}" \
    --output json 2>"$err_file") || return 0

  echo "$values" | jq -r --arg t "$table_name" --arg m "$metric" --arg s "$stat" \
    '.[] | [$t, $m, $s, .ts, (.val // 0)] | @csv' 2>/dev/null || true
}

get_exact_item_count() {
  local table_name="$1"
  local scan_result err_file
  err_file=$(mktemp "${TMPDIR:-/tmp}/dynamo-cw-err.XXXXXX")
  # shellcheck disable=SC2064
  trap "rm -f '$err_file'" RETURN

  scan_result=$(aws dynamodb scan \
    --region "$AWS_REGION" \
    --table-name "$table_name" \
    --select COUNT \
    --output json 2>"$err_file") || { echo ""; return; }

  echo "$scan_result" | jq '[.. | objects | .Count? // empty] | add // 0'
}

# ---------------------------------------------------------------------------
# PROCESSA UMA TABELA (executa em subshell paralelo)
# Cada tabela escreve em arquivos temporários próprios — sem race condition
# ---------------------------------------------------------------------------
process_table() {
  local TABLE_NAME="$1"
  local TABLE_FILE="$OUTPUT_DIR/${TABLE_NAME}.json"
  local TMP_SUMMARY="$TMP_DIR/${TABLE_NAME}.summary.csv"
  local TMP_INDEX="$TMP_DIR/${TABLE_NAME}.index.csv"
  local TMP_CW="$TMP_DIR/${TABLE_NAME}.cw.csv"

  log "→ Iniciando: $TABLE_NAME"

  # ── 1. describe-table ────────────────────────────────────────────────────
  local TABLE_DESC
  TABLE_DESC=$(aws dynamodb describe-table \
    --region "$AWS_REGION" \
    --table-name "$TABLE_NAME" \
    --output json 2>/dev/null) || { log "  AVISO: falha ao descrever $TABLE_NAME — pulando"; return 0; }

  local STATUS ITEM_COUNT TABLE_BYTES TABLE_MB AVG_ITEM BILLING TABLE_CLASS
  STATUS=$(echo      "$TABLE_DESC" | jq -r '.Table.TableStatus')
  ITEM_COUNT=$(echo  "$TABLE_DESC" | jq -r '.Table.ItemCount // 0')
  TABLE_BYTES=$(echo "$TABLE_DESC" | jq -r '.Table.TableSizeBytes // 0')
  TABLE_MB=$(echo    "$TABLE_DESC" | jq -r '(.Table.TableSizeBytes // 0) / 1048576 | . * 100 | round / 100')
  AVG_ITEM=$(echo    "$TABLE_DESC" | jq -r 'if (.Table.ItemCount // 0) > 0 then ((.Table.TableSizeBytes // 0) / .Table.ItemCount | round) else 0 end')
  BILLING=$(echo     "$TABLE_DESC" | jq -r '.Table.BillingModeSummary.BillingMode // "PROVISIONED"')
  TABLE_CLASS=$(echo "$TABLE_DESC" | jq -r '.Table.TableClassSummary.TableClass // "STANDARD"')

  if [[ "$EXACT_ITEM_COUNT" == "true" ]]; then
    local EXACT_COUNT
    EXACT_COUNT=$(get_exact_item_count "$TABLE_NAME")
    if [[ -n "$EXACT_COUNT" ]]; then
      ITEM_COUNT="$EXACT_COUNT"
      AVG_ITEM=$(echo "$TABLE_BYTES $ITEM_COUNT" | awk '{if ($2>0) printf "%.0f",$1/$2; else print 0}')
    fi
  fi

  local HASH_KEY RANGE_KEY
  HASH_KEY=$(echo  "$TABLE_DESC" | jq -r '.Table.KeySchema[] | select(.KeyType=="HASH")  | .AttributeName')
  RANGE_KEY=$(echo "$TABLE_DESC" | jq -r '.Table.KeySchema[] | select(.KeyType=="RANGE") | .AttributeName // ""')

  # função interna: captura TABLE_DESC do escopo local do subshell
  get_attr_type() {
    local attr="$1"
    echo "$TABLE_DESC" | jq -r --arg a "$attr" \
      '.Table.AttributeDefinitions[] | select(.AttributeName==$a) | .AttributeType // "?"'
  }

  local HASH_TYPE RANGE_TYPE
  HASH_TYPE=$(get_attr_type "$HASH_KEY")
  RANGE_TYPE=""
  [[ -n "$RANGE_KEY" ]] && RANGE_TYPE=$(get_attr_type "$RANGE_KEY")

  local PROV_RCU PROV_WCU
  PROV_RCU=$(echo "$TABLE_DESC" | jq -r '.Table.ProvisionedThroughput.ReadCapacityUnits  // 0')
  PROV_WCU=$(echo "$TABLE_DESC" | jq -r '.Table.ProvisionedThroughput.WriteCapacityUnits // 0')

  local GSI_COUNT GSI_JSON LSI_COUNT LSI_JSON
  GSI_COUNT=$(echo "$TABLE_DESC" | jq '.Table.GlobalSecondaryIndexes // [] | length')
  GSI_JSON=$(echo  "$TABLE_DESC" | jq '.Table.GlobalSecondaryIndexes // []')
  LSI_COUNT=$(echo "$TABLE_DESC" | jq '.Table.LocalSecondaryIndexes // [] | length')
  LSI_JSON=$(echo  "$TABLE_DESC" | jq '.Table.LocalSecondaryIndexes // []')

  local STREAMS STREAM_VIEW REPLICAS
  STREAMS=$(echo     "$TABLE_DESC" | jq -r '.Table.StreamSpecification.StreamEnabled // false')
  STREAM_VIEW=$(echo "$TABLE_DESC" | jq -r '.Table.StreamSpecification.StreamViewType // ""')
  REPLICAS=$(echo    "$TABLE_DESC" | jq -r '[.Table.Replicas[]?.RegionName] | join("|") // ""')

  # ── 2. TTL ───────────────────────────────────────────────────────────────
  local TTL_DESC TTL_STATUS TTL_ATTR TTL_ENABLED
  TTL_DESC=$(aws dynamodb describe-time-to-live \
    --region "$AWS_REGION" --table-name "$TABLE_NAME" \
    --output json 2>/dev/null || echo '{"TimeToLiveDescription":{}}')
  TTL_STATUS=$(echo "$TTL_DESC" | jq -r '.TimeToLiveDescription.TimeToLiveStatus // "DISABLED"')
  TTL_ATTR=$(echo   "$TTL_DESC" | jq -r '.TimeToLiveDescription.AttributeName    // ""')
  TTL_ENABLED="false"; [[ "$TTL_STATUS" == "ENABLED" ]] && TTL_ENABLED="true"

  # ── 3. PITR ──────────────────────────────────────────────────────────────
  local PITR_DESC PITR_STATUS PITR_ENABLED
  PITR_DESC=$(aws dynamodb describe-continuous-backups \
    --region "$AWS_REGION" --table-name "$TABLE_NAME" \
    --output json 2>/dev/null || echo '{"ContinuousBackupsDescription":{}}')
  PITR_STATUS=$(echo "$PITR_DESC" | jq -r \
    '.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus // "DISABLED"')
  PITR_ENABLED="false"; [[ "$PITR_STATUS" == "ENABLED" ]] && PITR_ENABLED="true"

  # ── 4. AUTO SCALING ──────────────────────────────────────────────────────
  local AS_POLICIES AS_READ_MIN AS_WRITE_MIN AS_READ AS_WRITE
  AS_POLICIES=$(aws application-autoscaling describe-scaling-policies \
    --region "$AWS_REGION" \
    --service-namespace dynamodb \
    --resource-id "table/${TABLE_NAME}" \
    --output json 2>/dev/null || echo '{"ScalingPolicies":[]}')
  AS_READ_MIN=$(echo  "$AS_POLICIES" | jq -r '[.ScalingPolicies[] | select(.ScalableDimension=="dynamodb:table:ReadCapacityUnits")  | .TargetTrackingScalingPolicyConfiguration.TargetValue] | first // ""')
  AS_WRITE_MIN=$(echo "$AS_POLICIES" | jq -r '[.ScalingPolicies[] | select(.ScalableDimension=="dynamodb:table:WriteCapacityUnits") | .TargetTrackingScalingPolicyConfiguration.TargetValue] | first // ""')
  AS_READ="false";  [[ -n "$AS_READ_MIN"  ]] && AS_READ="true (target ${AS_READ_MIN}%)"
  AS_WRITE="false"; [[ -n "$AS_WRITE_MIN" ]] && AS_WRITE="true (target ${AS_WRITE_MIN}%)"

  # ── 5. CLOUDWATCH — tabela ───────────────────────────────────────────────
  local DIMS_TABLE
  DIMS_TABLE="[{\"Name\":\"TableName\",\"Value\":\"${TABLE_NAME}\"}]"
  log "  → CloudWatch ($TABLE_NAME)..."

  local CW_RCU_TOTAL CW_WCU_TOTAL CW_RCU_AVG CW_WCU_AVG
  local CW_READ_LAT CW_WRITE_LAT
  local CW_THROTTLE_R CW_THROTTLE_W CW_SYS_ERR CW_TX_CONFLICT CW_RET_ITEMS CW_STORAGE_BYTES

  CW_RCU_TOTAL=$(get_cw_sum_all  "ConsumedReadCapacityUnits"  "Sum"     "$DIMS_TABLE")
  CW_WCU_TOTAL=$(get_cw_sum_all  "ConsumedWriteCapacityUnits" "Sum"     "$DIMS_TABLE")
  CW_RCU_AVG=$(echo "$CW_RCU_TOTAL $CW_DAYS" | awk '{printf "%.2f", $1/$2}')
  CW_WCU_AVG=$(echo "$CW_WCU_TOTAL $CW_DAYS" | awk '{printf "%.2f", $1/$2}')
  CW_READ_LAT=$(get_cw_metric    "SuccessfulRequestLatency"   "Average" "$DIMS_TABLE" \
                                 | awk '{printf "%.3f", $1}')
  CW_WRITE_LAT=$(get_cw_metric   "SuccessfulRequestLatency"   "Average" \
    "[{\"Name\":\"TableName\",\"Value\":\"${TABLE_NAME}\"},{\"Name\":\"Operation\",\"Value\":\"PutItem\"}]" \
    | awk '{printf "%.3f", $1}')
  CW_THROTTLE_R=$(get_cw_sum_all "ReadThrottleEvents"         "Sum"     "$DIMS_TABLE")
  CW_THROTTLE_W=$(get_cw_sum_all "WriteThrottleEvents"        "Sum"     "$DIMS_TABLE")
  CW_SYS_ERR=$(get_cw_sum_all    "SystemErrors"               "Sum"     "$DIMS_TABLE")
  CW_TX_CONFLICT=$(get_cw_sum_all "TransactionConflict"       "Sum"     "$DIMS_TABLE")
  CW_RET_ITEMS=$(get_cw_metric   "ReturnedItemCount"          "Average" "$DIMS_TABLE")
  CW_STORAGE_BYTES="$TABLE_BYTES"

  # ── 6. SÉRIE TEMPORAL → arquivo temporário da tabela ─────────────────────
  > "$TMP_CW"
  local METRIC_TS M S
  for METRIC_TS in "ConsumedReadCapacityUnits:Sum" "ConsumedWriteCapacityUnits:Sum" \
                   "ReadThrottleEvents:Sum" "WriteThrottleEvents:Sum" \
                   "SuccessfulRequestLatency:Average" "ReturnedItemCount:Average" \
                   "SystemErrors:Sum" "TransactionConflict:Sum"; do
    M="${METRIC_TS%%:*}"
    S="${METRIC_TS##*:}"
    get_cw_timeseries "$M" "$S" "$DIMS_TABLE" "$TABLE_NAME" >> "$TMP_CW" || true
  done

  # ── 7. ÍNDICES GSI → arquivo temporário da tabela ────────────────────────
  local GSI_SUMMARY="[]"
  > "$TMP_INDEX"
  if [[ "$GSI_COUNT" -gt 0 ]]; then
    GSI_SUMMARY=$(echo "$GSI_JSON" | jq -c '[.[] | {
      name:             .IndexName,
      hash_key:        (.KeySchema[] | select(.KeyType=="HASH")  | .AttributeName),
      range_key:       ((.KeySchema[] | select(.KeyType=="RANGE") | .AttributeName) // ""),
      projection_type: .Projection.ProjectionType,
      non_key_attrs:   (.Projection.NonKeyAttributes // [] | join("|")),
      size_bytes:      (.IndexSizeBytes  // 0),
      size_mb:         ((.IndexSizeBytes // 0) / 1048576 * 100 | round / 100),
      item_count:      (.ItemCount // 0),
      prov_rcu:        (.ProvisionedThroughput.ReadCapacityUnits  // 0),
      prov_wcu:        (.ProvisionedThroughput.WriteCapacityUnits // 0)
    }]')

    echo "$GSI_JSON" | jq -r '.[].IndexName' | while read -r GSI_NAME; do
      local DIMS_GSI
      DIMS_GSI="[{\"Name\":\"TableName\",\"Value\":\"${TABLE_NAME}\"},{\"Name\":\"GlobalSecondaryIndexName\",\"Value\":\"${GSI_NAME}\"}]"
      local GSI_RCU GSI_WCU GSI_TR GSI_TW
      GSI_RCU=$(get_cw_sum_all "ConsumedReadCapacityUnits"  "Sum" "$DIMS_GSI")
      GSI_WCU=$(get_cw_sum_all "ConsumedWriteCapacityUnits" "Sum" "$DIMS_GSI")
      GSI_TR=$(get_cw_sum_all  "ReadThrottleEvents"         "Sum" "$DIMS_GSI")
      GSI_TW=$(get_cw_sum_all  "WriteThrottleEvents"        "Sum" "$DIMS_GSI")

      local GSI_HASH GSI_RANGE GSI_HASH_T GSI_RANGE_T
      GSI_HASH=$(echo "$GSI_JSON" | jq -r --arg n "$GSI_NAME" \
        '.[] | select(.IndexName==$n) | .KeySchema[] | select(.KeyType=="HASH") | .AttributeName')
      GSI_RANGE=$(echo "$GSI_JSON" | jq -r --arg n "$GSI_NAME" \
        '.[] | select(.IndexName==$n) | .KeySchema[] | select(.KeyType=="RANGE") | .AttributeName // ""')
      GSI_HASH_T=$(get_attr_type "$GSI_HASH")
      GSI_RANGE_T=""; [[ -n "$GSI_RANGE" ]] && GSI_RANGE_T=$(get_attr_type "$GSI_RANGE")

      local GSI_PROJ GSI_PROJ_ATTRS GSI_SIZE GSI_SIZE_MB GSI_ITEMS GSI_RCU_P GSI_WCU_P
      GSI_PROJ=$(echo "$GSI_JSON" | jq -r --arg n "$GSI_NAME" \
        '.[] | select(.IndexName==$n) | .Projection.ProjectionType')
      GSI_PROJ_ATTRS=$(echo "$GSI_JSON" | jq -r --arg n "$GSI_NAME" \
        '.[] | select(.IndexName==$n) | (.Projection.NonKeyAttributes // []) | join("|")')
      GSI_SIZE=$(echo "$GSI_JSON" | jq -r --arg n "$GSI_NAME" \
        '.[] | select(.IndexName==$n) | (.IndexSizeBytes // 0)')
      GSI_SIZE_MB=$(echo "$GSI_SIZE" | awk '{printf "%.2f", $1/1048576}')
      GSI_ITEMS=$(echo "$GSI_JSON" | jq -r --arg n "$GSI_NAME" \
        '.[] | select(.IndexName==$n) | (.ItemCount // 0)')
      GSI_RCU_P=$(echo "$GSI_JSON" | jq -r --arg n "$GSI_NAME" \
        '.[] | select(.IndexName==$n) | (.ProvisionedThroughput.ReadCapacityUnits  // 0)')
      GSI_WCU_P=$(echo "$GSI_JSON" | jq -r --arg n "$GSI_NAME" \
        '.[] | select(.IndexName==$n) | (.ProvisionedThroughput.WriteCapacityUnits // 0)')

      echo "\"${TABLE_NAME}\",\"GSI\",\"${GSI_NAME}\",\"${GSI_HASH}\",\"${GSI_HASH_T}\",\
\"${GSI_RANGE}\",\"${GSI_RANGE_T}\",\"${GSI_PROJ}\",\"${GSI_PROJ_ATTRS}\",\
${GSI_SIZE},${GSI_SIZE_MB},${GSI_ITEMS},\
${GSI_RCU_P},${GSI_WCU_P},\
${GSI_RCU},${GSI_WCU},\
${GSI_TR},${GSI_TW}" >> "$TMP_INDEX"
    done
  fi

  # ── 8. ÍNDICES LSI ───────────────────────────────────────────────────────
  local LSI_SUMMARY="[]"
  if [[ "$LSI_COUNT" -gt 0 ]]; then
    LSI_SUMMARY=$(echo "$LSI_JSON" | jq -c '[.[] | {
      name:             .IndexName,
      range_key:        (.KeySchema[] | select(.KeyType=="RANGE") | .AttributeName),
      projection_type:  .Projection.ProjectionType,
      non_key_attrs:    (.Projection.NonKeyAttributes // [] | join("|")),
      size_bytes:       (.IndexSizeBytes  // 0),
      size_mb:          ((.IndexSizeBytes // 0) / 1048576 * 100 | round / 100),
      item_count:       (.ItemCount // 0)
    }]')

    echo "$LSI_JSON" | jq -r '.[].IndexName' | while read -r LSI_NAME; do
      local LSI_RANGE LSI_RANGE_T LSI_PROJ LSI_PROJ_ATTRS LSI_SIZE LSI_SIZE_MB LSI_ITEMS
      LSI_RANGE=$(echo "$LSI_JSON" | jq -r --arg n "$LSI_NAME" \
        '.[] | select(.IndexName==$n) | .KeySchema[] | select(.KeyType=="RANGE") | .AttributeName')
      LSI_RANGE_T=$(get_attr_type "$LSI_RANGE")
      LSI_PROJ=$(echo "$LSI_JSON" | jq -r --arg n "$LSI_NAME" \
        '.[] | select(.IndexName==$n) | .Projection.ProjectionType')
      LSI_PROJ_ATTRS=$(echo "$LSI_JSON" | jq -r --arg n "$LSI_NAME" \
        '.[] | select(.IndexName==$n) | (.Projection.NonKeyAttributes // []) | join("|")')
      LSI_SIZE=$(echo "$LSI_JSON" | jq -r --arg n "$LSI_NAME" \
        '.[] | select(.IndexName==$n) | (.IndexSizeBytes // 0)')
      LSI_SIZE_MB=$(echo "$LSI_SIZE" | awk '{printf "%.2f", $1/1048576}')
      LSI_ITEMS=$(echo "$LSI_JSON" | jq -r --arg n "$LSI_NAME" \
        '.[] | select(.IndexName==$n) | (.ItemCount // 0)')

      echo "\"${TABLE_NAME}\",\"LSI\",\"${LSI_NAME}\",\"${HASH_KEY}\",\"${HASH_TYPE}\",\
\"${LSI_RANGE}\",\"${LSI_RANGE_T}\",\"${LSI_PROJ}\",\"${LSI_PROJ_ATTRS}\",\
${LSI_SIZE},${LSI_SIZE_MB},${LSI_ITEMS},\
0,0,0,0,0,0" >> "$TMP_INDEX"
    done
  fi

  # ── 9. JSON da tabela ────────────────────────────────────────────────────
  jq -n \
    --arg  table_name       "$TABLE_NAME" \
    --arg  status           "$STATUS" \
    --arg  billing          "$BILLING" \
    --arg  table_class      "$TABLE_CLASS" \
    --argjson item_count    "$ITEM_COUNT" \
    --argjson table_bytes   "$TABLE_BYTES" \
    --argjson table_mb      "$TABLE_MB" \
    --argjson avg_item      "$AVG_ITEM" \
    --arg  hash_key         "$HASH_KEY" \
    --arg  hash_type        "$HASH_TYPE" \
    --arg  range_key        "$RANGE_KEY" \
    --arg  range_type       "$RANGE_TYPE" \
    --argjson prov_rcu      "$PROV_RCU" \
    --argjson prov_wcu      "$PROV_WCU" \
    --arg  as_read          "$AS_READ" \
    --arg  as_write         "$AS_WRITE" \
    --arg  ttl_enabled      "$TTL_ENABLED" \
    --arg  ttl_attr         "$TTL_ATTR" \
    --arg  streams          "$STREAMS" \
    --arg  stream_view      "$STREAM_VIEW" \
    --arg  pitr             "$PITR_ENABLED" \
    --arg  replicas         "$REPLICAS" \
    --argjson gsi_count     "$GSI_COUNT" \
    --argjson lsi_count     "$LSI_COUNT" \
    --argjson gsi_detail    "$GSI_SUMMARY" \
    --argjson lsi_detail    "$LSI_SUMMARY" \
    --argjson cw_rcu_total  "$CW_RCU_TOTAL" \
    --argjson cw_wcu_total  "$CW_WCU_TOTAL" \
    --arg  cw_rcu_avg       "$CW_RCU_AVG" \
    --arg  cw_wcu_avg       "$CW_WCU_AVG" \
    --arg  cw_read_lat      "$CW_READ_LAT" \
    --arg  cw_write_lat     "$CW_WRITE_LAT" \
    --argjson cw_thr_r      "$CW_THROTTLE_R" \
    --argjson cw_thr_w      "$CW_THROTTLE_W" \
    --argjson cw_syserr     "$CW_SYS_ERR" \
    --argjson cw_txconf     "$CW_TX_CONFLICT" \
    --arg  cw_ret_items     "$CW_RET_ITEMS" \
    --arg  cw_storage       "$CW_STORAGE_BYTES" \
    '{
      table_name:       $table_name,
      status:           $status,
      billing_mode:     $billing,
      table_class:      $table_class,
      sizing: {
        item_count:          $item_count,
        table_size_bytes:    $table_bytes,
        table_size_mb:       $table_mb,
        avg_item_size_bytes: $avg_item
      },
      primary_key: {
        hash_key:   $hash_key,
        hash_type:  $hash_type,
        range_key:  $range_key,
        range_type: $range_type
      },
      capacity: {
        provisioned_rcu:   $prov_rcu,
        provisioned_wcu:   $prov_wcu,
        autoscaling_read:  $as_read,
        autoscaling_write: $as_write
      },
      features: {
        ttl_enabled:          $ttl_enabled,
        ttl_attribute:        $ttl_attr,
        streams_enabled:      $streams,
        stream_view_type:     $stream_view,
        pitr_enabled:         $pitr,
        global_table_regions: $replicas
      },
      indexes: {
        gsi_count:  $gsi_count,
        lsi_count:  $lsi_count,
        gsi_detail: $gsi_detail,
        lsi_detail: $lsi_detail
      },
      cloudwatch_60d: {
        consumed_rcu_total:     $cw_rcu_total,
        consumed_wcu_total:     $cw_wcu_total,
        consumed_rcu_avg_day:   $cw_rcu_avg,
        consumed_wcu_avg_day:   $cw_wcu_avg,
        read_latency_avg_ms:    $cw_read_lat,
        write_latency_avg_ms:   $cw_write_lat,
        throttled_reads_total:  $cw_thr_r,
        throttled_writes_total: $cw_thr_w,
        system_errors_total:    $cw_syserr,
        transaction_conflicts:  $cw_txconf,
        returned_items_avg:     $cw_ret_items,
        storage_bytes_max:      $cw_storage
      }
    }' > "$TABLE_FILE"

  # ── 10. Linha do summary CSV → arquivo temporário da tabela ──────────────
  echo "\"${TABLE_NAME}\",\"${BILLING}\",\"${TABLE_CLASS}\",\"${STATUS}\",\
${ITEM_COUNT},${TABLE_BYTES},${TABLE_MB},\
${AVG_ITEM},${PROV_RCU},${PROV_WCU},\
\"${AS_READ}\",\"${AS_WRITE}\",\
${TTL_ENABLED},\"${TTL_ATTR}\",\
${STREAMS},\"${STREAM_VIEW}\",\
${PITR_ENABLED},\
\"${REPLICAS}\",\
${GSI_COUNT},${LSI_COUNT},\
${CW_RCU_TOTAL},${CW_WCU_TOTAL},\
${CW_RCU_AVG},${CW_WCU_AVG},\
${CW_READ_LAT},${CW_WRITE_LAT},\
${CW_THROTTLE_R},${CW_THROTTLE_W},\
${CW_SYS_ERR},${CW_TX_CONFLICT},\
${CW_RET_ITEMS},${CW_STORAGE_BYTES}" > "$TMP_SUMMARY"

  log "  ✓ $TABLE_NAME — ${ITEM_COUNT} itens | ${TABLE_MB} MB | GSI: ${GSI_COUNT} | LSI: ${LSI_COUNT}"
  hr
}

# ---------------------------------------------------------------------------
# LISTA TODAS AS TABELAS DA REGIÃO
# ---------------------------------------------------------------------------
log "Listando tabelas DynamoDB na região $AWS_REGION..."

ALL_TABLES=()
LAST_EVAL=""

while true; do
  local_args=(--region "$AWS_REGION" --max-items 100 --output json)
  [[ -n "$LAST_EVAL" ]] && local_args+=(--starting-token "$LAST_EVAL")

  RESULT=$(aws dynamodb list-tables "${local_args[@]}" 2>/dev/null) \
    || die "falha ao listar tabelas DynamoDB na região $AWS_REGION"

  while IFS= read -r t; do ALL_TABLES+=("$t"); done \
    < <(echo "$RESULT" | jq -r '.TableNames[]')

  LAST_EVAL=$(echo "$RESULT" | jq -r '.NextToken // empty')
  [[ -z "$LAST_EVAL" ]] && break
done

TABLE_COUNT=${#ALL_TABLES[@]}
if [[ -n "$TABLE_PREFIX" ]]; then
  FILTERED_TABLES=()
  for t in "${ALL_TABLES[@]}"; do
    [[ "$t" == "$TABLE_PREFIX"* ]] && FILTERED_TABLES+=("$t")
  done
  ALL_TABLES=("${FILTERED_TABLES[@]}")
  TABLE_COUNT=${#ALL_TABLES[@]}
  log "Filtro TABLE_PREFIX=$TABLE_PREFIX aplicado."
fi
log "Encontrada(s) $TABLE_COUNT tabela(s). Paralelismo: $MAX_PARALLEL"
hr

# ---------------------------------------------------------------------------
# CABEÇALHOS DOS CSV
# ---------------------------------------------------------------------------
echo "table_name,billing_mode,table_class,status,item_count,table_size_bytes,table_size_mb,\
avg_item_size_bytes,provisioned_rcu,provisioned_wcu,autoscaling_read,autoscaling_write,\
ttl_enabled,ttl_attribute,streams_enabled,stream_view_type,pitr_enabled,\
global_table_regions,gsi_count,lsi_count,\
cw_consumed_rcu_total,cw_consumed_wcu_total,cw_consumed_rcu_avg_day,cw_consumed_wcu_avg_day,\
cw_read_latency_avg_ms,cw_write_latency_avg_ms,\
cw_throttled_reads_total,cw_throttled_writes_total,\
cw_system_errors_total,cw_transaction_conflicts_total,\
cw_returned_items_avg,cw_storage_bytes_max" \
> "$SUMMARY_CSV"

echo "table_name,index_type,index_name,hash_key,hash_key_type,\
range_key,range_key_type,projection_type,projected_attributes,\
index_size_bytes,index_size_mb,item_count,\
provisioned_rcu,provisioned_wcu,\
cw_consumed_rcu_total,cw_consumed_wcu_total,\
cw_throttled_reads_total,cw_throttled_writes_total" \
> "$INDEX_CSV"

echo "table_name,metric,statistic,timestamp,value" > "$CW_CSV"

# ---------------------------------------------------------------------------
# SEMÁFORO FIFO — controla MAX_PARALLEL subshells simultâneos
# Compatível com bash 3.2+ (macOS e Linux)
# ---------------------------------------------------------------------------
_SEM_FIFO=$(mktemp -u "${TMPDIR:-/tmp}/dynamo-sem.XXXXXX")
mkfifo "$_SEM_FIFO"
exec 3<>"$_SEM_FIFO"
rm -f "$_SEM_FIFO"
trap 'exec 3>&-' EXIT

# Preenche o semáforo com MAX_PARALLEL tokens (um \n por slot)
for _i in $(seq 1 "$MAX_PARALLEL"); do printf '\n' >&3; done

# ---------------------------------------------------------------------------
# DISPARA PROCESSAMENTO PARALELO
# ---------------------------------------------------------------------------
for TABLE_NAME in "${ALL_TABLES[@]}"; do
  read -r -u3         # aguarda um slot livre (bloqueia se MAX_PARALLEL atingido)
  (
    process_table "$TABLE_NAME" || true   # || true garante liberação do slot mesmo em falha
    printf '\n' >&3   # libera o slot
  ) &
done

wait   # aguarda todos os subshells terminarem
log "Todos os processamentos concluídos."
hr

# ---------------------------------------------------------------------------
# CONSOLIDA CSVs na ordem original das tabelas (evita output entrelaçado)
# ---------------------------------------------------------------------------
log "Consolidando CSVs..."
for TABLE_NAME in "${ALL_TABLES[@]}"; do
  [[ -f "$TMP_DIR/${TABLE_NAME}.summary.csv" ]] && cat "$TMP_DIR/${TABLE_NAME}.summary.csv" >> "$SUMMARY_CSV"
  [[ -f "$TMP_DIR/${TABLE_NAME}.index.csv"   ]] && cat "$TMP_DIR/${TABLE_NAME}.index.csv"   >> "$INDEX_CSV"
  [[ -f "$TMP_DIR/${TABLE_NAME}.cw.csv"      ]] && cat "$TMP_DIR/${TABLE_NAME}.cw.csv"      >> "$CW_CSV"
done
rm -rf "$TMP_DIR"

# ---------------------------------------------------------------------------
# CONSOLIDA RELATÓRIO FINAL
# ---------------------------------------------------------------------------
log "Consolidando relatório JSON..."
ALL_JSON_FILES=()
for TABLE_NAME in "${ALL_TABLES[@]}"; do
  TABLE_FILE="$OUTPUT_DIR/${TABLE_NAME}.json"
  [[ -f "$TABLE_FILE" ]] && ALL_JSON_FILES+=("$TABLE_FILE")
done

if [[ ${#ALL_JSON_FILES[@]} -gt 0 ]]; then
  jq -s '.' "${ALL_JSON_FILES[@]}" > "$REPORT_FILE"
else
  echo "[]" > "$REPORT_FILE"
fi

# ---------------------------------------------------------------------------
# SUMÁRIO NO TERMINAL
# ---------------------------------------------------------------------------
log ""
log "════════════════════════════════════════════════════"
log "SUMÁRIO GERAL"
log "════════════════════════════════════════════════════"
jq -r '.[] |
  "Tabela: \(.table_name)
  Modo:    \(.billing_mode) | Classe: \(.table_class) | Status: \(.status)
  Itens:   \(.sizing.item_count) | Tamanho: \(.sizing.table_size_mb) MB | Avg item: \(.sizing.avg_item_size_bytes) bytes
  GSIs:    \(.indexes.gsi_count) | LSIs: \(.indexes.lsi_count)
  RCU 60d: \(.cloudwatch_60d.consumed_rcu_total) (avg \(.cloudwatch_60d.consumed_rcu_avg_day)/dia)
  WCU 60d: \(.cloudwatch_60d.consumed_wcu_total) (avg \(.cloudwatch_60d.consumed_wcu_avg_day)/dia)
  Throttle R/W: \(.cloudwatch_60d.throttled_reads_total) / \(.cloudwatch_60d.throttled_writes_total)
  "' "$REPORT_FILE" 2>/dev/null || true

log "════════════════════════════════════════════════════"
log "Arquivos gerados em: $OUTPUT_DIR"
log "  Relatório JSON      : $REPORT_FILE"
log "  Resumo CSV          : $SUMMARY_CSV"
log "  Índices CSV         : $INDEX_CSV"
log "  Séries CW CSV       : $CW_CSV"
log "════════════════════════════════════════════════════"
