#!/usr/bin/env bash
# End-to-end check of the whole stack with docker compose, curl and jq.
#
# Scenarios: the happy path; insufficient funds; a screening decline that
# releases the hold; a review that is polled and approved; a screening
# provider that executes and hangs, so the saga retries with the same key
# and completes once; notifications delivered once although the relay
# publishes every event twice; a poison message that ends in the
# dead-letter topic and fires its alert. After each one the ledger's books
# are checked with SQL.
#
# Requires docker compose, curl and jq. KEEP=1 leaves the stack running;
# NO_BUILD=1 skips the image build.
set -euo pipefail
cd "$(dirname "$0")/.."

API=${API:-http://localhost:8080}
NOTIFY=${NOTIFY:-http://localhost:8081}
RISK=${RISK:-http://localhost:8090}
PROM=${PROM:-http://localhost:9091}
KEEP=${KEEP:-0}

pass=0
log() { printf '%s\n' "$*" >&2; }
ok() { pass=$((pass + 1)); log "  ok: $*"; }
fail() {
  log "FAIL: $*"
  docker compose logs --tail=60 transfers ledger notifications fakerisk >&2 || true
  exit 1
}
expect() { [ "$2" = "$3" ] || fail "$1: got '$2', want '$3'"; ok "$1"; }
at_least() {
  awk -v v="${2:-}" -v w="$3" 'BEGIN { exit !(v != "" && v + 0 >= w) }' || fail "$1: got '${2:-missing}', want >= $3"
  ok "$1"
}

cleanup() {
  if [ "$KEEP" != 1 ]; then
    docker compose down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

sql() { docker compose exec -T postgres psql -U postgres -d "$1" -tAc "$2"; }
metric() { curl -sS "$1/metrics" | awk -v s="$2" '$1 == s { print $2 }'; }
admin_transfers=http://localhost:9100
admin_ledger=http://localhost:9101
admin_notifications=http://localhost:9102

# eventually CMD... retries a command for up to 60 s.
eventually() {
  local deadline=$((SECONDS + 60))
  until "$@"; do
    [ $SECONDS -lt $deadline ] || return 1
    sleep 0.5
  done
}

account_id() { curl -sS "$API/v1/accounts" | jq -r --arg n "$1" '.accounts[] | select(.name == $n) | .id'; }
available() { curl -sS "$API/v1/accounts" | jq -r --arg id "$1" '.accounts[] | select(.id == $id) | .available_minor'; }
posted() { curl -sS "$API/v1/accounts" | jq -r --arg id "$1" '.accounts[] | select(.id == $id) | .posted_minor'; }

# create KEY SRC DST AMOUNT -> prints "HTTP_CODE BODY"
create() {
  local body code
  body=$(mktemp)
  code=$(curl -sS -o "$body" -w '%{http_code}' -X POST "$API/v1/transfers" \
    -H "Idempotency-Key: $1" -H 'Content-Type: application/json' \
    -d "{\"source_account_id\":\"$2\",\"destination_account_id\":\"$3\",\"amount_minor\":$4,\"currency\":\"EUR\"}")
  printf '%s %s\n' "$code" "$(cat "$body")"
  rm -f "$body"
}

# new_transfer SRC DST AMOUNT -> prints the id of a freshly created transfer
new_transfer() {
  local out
  out=$(create "e2e-$RANDOM-$RANDOM-$SECONDS" "$1" "$2" "$3")
  [ "${out%% *}" = 201 ] || fail "create returned $out"
  jq -r .id <<<"${out#* }"
}

# follow ID -> long-polls until the transfer is final, prints "STATUS REASON"
follow() {
  local id=$1 version=0 deadline=$((SECONDS + 120)) resp status
  while [ $SECONDS -lt $deadline ]; do
    resp=$(curl -sS --max-time 40 "$API/v1/transfers/$id/wait?version=$version") || continue
    version=$(jq -r .version <<<"$resp")
    status=$(jq -r .status <<<"$resp")
    if [ "$(jq -r .modified <<<"$resp")" = true ]; then
      log "    $id v$version $status $(jq -c '[.steps[] | "\(.step):\(.state)\(if .outcome then "/" + .outcome else "" end)"]' <<<"$resp")"
    fi
    case $status in completed | rejected | canceled)
      echo "$status $(jq -r '.reason // ""' <<<"$resp")"
      return 0
      ;;
    esac
  done
  fail "transfer $id did not reach a final status in time"
}

hold_key() { echo "transfer:$1:hold"; }

# books_balanced checks the double-entry invariants in the ledger database.
books_balanced() {
  expect "every ledger transaction balances" \
    "$(sql ledger "SELECT count(*) FROM (SELECT transaction_id FROM entries GROUP BY transaction_id HAVING sum(amount_minor) <> 0) x")" 0
  expect "posted balances of all accounts sum to zero" "$(sql ledger "SELECT coalesce(sum(posted_minor), 0) FROM accounts")" 0
  expect "customer money is constant" "$(sql ledger "SELECT sum(posted_minor) FROM accounts WHERE kind = 'customer'")" "$customer_total"
  expect "no customer is overdrawn" "$(sql ledger "SELECT count(*) FROM accounts WHERE kind = 'customer' AND posted_minor < held_minor")" 0
}

log "==> starting the stack"
if [ "${NO_BUILD:-0}" != 1 ]; then
  docker compose build
fi
docker compose up -d --wait
for port in 9100 9101 9102; do
  expect "readyz on admin port $port" "$(curl -sS -o /dev/null -w '%{http_code}' "http://localhost:$port/readyz")" 200
done
curl -sS "$API/" | grep -q "Pinkponies Bank" || fail "web UI does not render"
ok "web UI page"
for asset in app.js style.css logo.svg config.json; do
  expect "asset $asset" "$(curl -sS -o /dev/null -w '%{http_code}' "$API/$asset")" 200
done
expect "the UI knows where the notifications API is" "$(curl -sS "$API/config.json" | jq -r .notifications_url)" "$NOTIFY"

clover=$(account_id "Clover Hayfield")
pip=$(account_id "Pip Sugarmane")
juniper=$(account_id "Juniper Brook")
[ -n "$clover" ] && [ -n "$pip" ] && [ -n "$juniper" ] || fail "seed accounts missing"
ok "seed accounts listed through the ledger's gRPC API"
customer_total=$(sql ledger "SELECT sum(posted_minor) FROM accounts WHERE kind = 'customer'")

log "==> happy path, idempotent create"
clover0=$(posted "$clover")
pip0=$(posted "$pip")
out=$(create e2e-happy "$clover" "$pip" 1250)
expect "first create" "${out%% *}" 201
happy=$(jq -r .id <<<"${out#* }")
out=$(create e2e-happy "$clover" "$pip" 1250)
expect "replay with the same key" "${out%% *}" 200
expect "replay returns the same transfer" "$(jq -r .id <<<"${out#* }")" "$happy"
out=$(create e2e-happy "$clover" "$pip" 1251)
expect "same key, different body" "${out%% *}" 422
expect "happy path" "$(follow "$happy")" "completed "
expect "source debited once" "$(posted "$clover")" "$((clover0 - 1250))"
expect "destination credited once" "$(posted "$pip")" "$((pip0 + 1250))"
expect "one capture in the ledger" \
  "$(sql ledger "SELECT count(*) FROM transactions t JOIN holds h ON h.id = t.hold_id WHERE h.idempotency_key = '$(hold_key "$happy")'")" 1
books_balanced

log "==> insufficient funds: rejected, nothing held, nothing to compensate"
juniper0=$(available "$juniper")
id=$(new_transfer "$juniper" "$pip" 600)
expect "insufficient funds" "$(follow "$id")" "rejected insufficient_funds"
expect "no hold was placed" "$(sql ledger "SELECT count(*) FROM holds WHERE idempotency_key = '$(hold_key "$id")'")" 0
expect "available balance unchanged" "$(available "$juniper")" "$juniper0"

log "==> screening declines: the hold is released"
clover0=$(posted "$clover")
id=$(new_transfer "$clover" "$pip" 1266)
expect "decline compensates" "$(follow "$id")" "rejected screening_declined"
expect "hold released" "$(sql ledger "SELECT status FROM holds WHERE idempotency_key = '$(hold_key "$id")'")" released
expect "no money moved" "$(posted "$clover")" "$clover0"
expect "the saga ran the release step" \
  "$(sql transfers "SELECT state FROM saga_steps WHERE transfer_id = '$id' AND step = 'release'")" succeeded
books_balanced

log "==> screening reviews, then approves: polled as a delayed retry"
id=$(new_transfer "$clover" "$pip" 1277)
expect "review then approve" "$(follow "$id")" "completed "
at_least "screen step polled more than once" \
  "$(sql transfers "SELECT attempts FROM saga_steps WHERE transfer_id = '$id' AND step = 'screen'")" 2

log "==> the provider executes the screening and hangs: same-key retry, completed once"
executions0=$(curl -sS "$RISK/_control" | jq -r '.stats.executions // 0')
curl -sS -X POST "$RISK/_control?faults=hang" >/dev/null
clover0=$(posted "$clover")
id=$(new_transfer "$clover" "$pip" 2000)
expect "completed after the hang" "$(follow "$id")" "completed "
screening=$(curl -sS "$RISK/_control/screening?key=transfer:$id:screen")
at_least "screening asked again with the same key" "$(jq -r .calls <<<"$screening")" 2
expect "the provider screened once" "$(($(curl -sS "$RISK/_control" | jq -r .stats.executions) - executions0))" 1
expect "captured once" \
  "$(sql ledger "SELECT count(*) FROM transactions t JOIN holds h ON h.id = t.hold_id WHERE h.idempotency_key = '$(hold_key "$id")'")" 1
expect "debited once" "$(posted "$clover")" "$((clover0 - 2000))"
at_least "the ambiguous call was counted" "$(metric $admin_transfers 'screening_requests_total{outcome="ambiguous"}')" 1
books_balanced

log "==> notifications are delivered once although every event is published twice"
notes_of() { sql notifications "SELECT count(*) FROM notifications WHERE transfer_id = '$1'"; }
three_notes() { [ "$(notes_of "$happy")" = 3 ]; }
eventually three_notes || fail "the happy transfer did not get its 3 notifications (accepted, sent, received)"
ok "three notifications for the happy transfer"
duplicates0=$(metric $admin_notifications 'consumer_records_total{outcome="duplicate"}')
# Pretend the relay crashed after Kafka acknowledged and before it marked
# the events: they are published again under the same event ids.
sql transfers "UPDATE outbox SET published_at = NULL WHERE aggregate_id = '$happy'" >/dev/null
republished() { [ "$(sql transfers "SELECT count(*) FROM outbox WHERE aggregate_id = '$happy' AND published_at IS NULL")" = 0 ]; }
eventually republished || fail "the relay did not publish the events again"
ok "the relay published the transfer's 3 events a second time"
dupes_seen() { awk -v a="$(metric $admin_notifications 'consumer_records_total{outcome="duplicate"}')" -v b="$duplicates0" 'BEGIN { exit !(a - b >= 3) }'; }
eventually dupes_seen || fail "the consumer did not see the 3 duplicates"
ok "the consumer saw the 3 duplicates"
expect "still three notifications" "$(notes_of "$happy")" 3
delivered() {
  [ "$(sql notifications "SELECT count(*) FROM deliveries d JOIN notifications n ON n.id = d.notification_id
        WHERE n.transfer_id = '$happy' AND d.state = 'delivered'")" = 6 ]
}
eventually delivered || fail "the 3 notifications were not delivered on both channels"
ok "delivered on both channels"
sink=$(curl -sS "$NOTIFY/_sink/webhooks")
for n in $(sql notifications "SELECT id FROM notifications WHERE transfer_id = '$happy'"); do
  expect "webhook for notification $n received once" "$(jq -r --arg k "$n" '.by_key[$k] // 0' <<<"$sink")" 1
done
feed=$(curl -sS -H "Origin: http://localhost:8080" -D /tmp/ppb-e2e-headers "$NOTIFY/v1/notifications?account=$pip")
grep -qi "^access-control-allow-origin: http://localhost:8080" /tmp/ppb-e2e-headers || fail "the feed is not readable from the UI's origin"
ok "the feed answers the UI's origin with CORS"
at_least "the receiver's feed has its notification" "$(jq '[.notifications[] | select(.kind == "transfer_received")] | length' <<<"$feed")" 1

log "==> a poison message ends in the dead-letter topic and fires its alert"
echo 'this is not a transfer event' | docker compose exec -T redpanda rpk topic produce transfers.events -k poison -X brokers=redpanda:9092 >/dev/null
dead_lettered() { [ "$(metric $admin_notifications 'kafka_topic_messages{topic="transfers.events.dlq"}')" = 1 ]; }
eventually dead_lettered || fail "the poison message did not reach the dead-letter topic"
ok "dead-letter topic holds the poison message"
dlq=$(docker compose exec -T redpanda rpk topic consume transfers.events.dlq -n 1 -o start -f '%v|%h{%k=%v;}\n' -X brokers=redpanda:9092)
grep -q '^this is not a transfer event|' <<<"$dlq" || fail "dead letter = $dlq"
grep -q 'dlq-attempts=5;' <<<"$dlq" || fail "dead letter headers = $dlq"
ok "dead letter keeps the value and says why ($(sed 's/.*dlq-error=\([^;]*\);.*/\1/' <<<"$dlq" | cut -c1-60)...)"
alert_state() { curl -sS "$PROM/api/v1/alerts" | jq -r '.data.alerts[] | select(.labels.alertname == "DLQNotEmpty") | .state'; }
alerting() { case "$(alert_state)" in pending | firing) return 0 ;; esac; return 1; }
eventually alerting || fail "DLQNotEmpty is not pending or firing"
ok "Prometheus alert DLQNotEmpty is $(alert_state)"
after_poison=$(new_transfer "$clover" "$pip" 300)
expect "the consumer goes on after the poison message" "$(follow "$after_poison")" "completed "
two_more() { [ "$(notes_of "$after_poison")" = 3 ]; }
eventually two_more || fail "no notifications after the poison message"
ok "notifications keep flowing"

log "==> final state"
drained() { [ "$(sql transfers "SELECT count(*) FROM outbox WHERE published_at IS NULL")" = 0 ]; }
eventually drained || fail "outbox events left unpublished"
ok "outbox drained ($(sql transfers "SELECT count(*) FROM outbox") events)"
expect "no dead saga tasks" "$(sql transfers "SELECT count(*) FROM tasks WHERE state = 'dead'")" 0
expect "no dead delivery tasks" "$(sql notifications "SELECT count(*) FROM tasks WHERE state = 'dead'")" 0
expect "no active holds left" "$(sql ledger "SELECT count(*) FROM holds WHERE status = 'active'")" 0
books_balanced
expect "transfers created: six, the replay of e2e-happy not counted" "$(metric $admin_transfers transfers_created_total)" 6
at_least "ledger placed holds over gRPC" \
  "$(metric $admin_ledger 'grpc_server_handled_total{code="OK",method="/ledger.v1.LedgerService/PlaceHold"}')" 5
at_least "ledger refused the overdraft" \
  "$(metric $admin_ledger 'grpc_server_handled_total{code="FailedPrecondition",method="/ledger.v1.LedgerService/PlaceHold"}')" 1
expect "ledger books balance in the metrics" \
  "$(($(metric $admin_ledger 'ledger_balance_sum_minor{currency="EUR",kind="customer"}' | cut -d. -f1) + $(metric $admin_ledger 'ledger_balance_sum_minor{currency="EUR",kind="system"}' | cut -d. -f1)))" 0
up_targets=$(curl -sS "$PROM/api/v1/query" --data-urlencode 'query=sum(up)' | jq -r '.data.result[0].value[1] // 0')
expect "Prometheus scrapes all three services" "$up_targets" 3

log "PASS: $pass checks"
