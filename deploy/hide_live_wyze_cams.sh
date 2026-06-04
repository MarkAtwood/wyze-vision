#!/usr/bin/env bash
#
# Hide the 20 live ha-wyzeapi camera.<key> entities from the auto-generated
# default Overview dashboard.
#
# WHY: the live wyzeapi cameras are WebRTC-only -- /api/camera_proxy/<cam>
# returns HTTP 500, so they render as GREY placeholder tiles on the auto-gen
# Overview (one card per camera.* entity, using the entity as its own image
# source -- no camera_image hook to swap in a still). The wyze-snapshot sidecar
# publishes working stills as camera.wyze_<key>_snapshot (local_file, HTTP 200
# JPEG), which the Overview ALSO auto-generates cards for. Marking the live cams
# hidden_by=user makes the Overview drop them, leaving only the working stills.
#
# HIDDEN, NOT DISABLED: hidden_by keeps the entity fully functional/available
# (live WebRTC still plays from the dedicated Cameras dashboard); it is only
# excluded from auto-generated dashboards. Reversible by unhiding.
#
# WS, NOT REST: hidden_by is set via the WebSocket command
# config/entity_registry/update, which has no REST equivalent (unlike the
# config-flow REST used by provision_local_file_cameras.sh). Uses websocat.
#
# Idempotent: entities already hidden_by=user are skipped, so it is safe to
# re-run. Requires an ADMIN token (entity registry is admin-only); read from the
# macOS Keychain, never hardcoded.
#
# Usage: ./hide_live_wyze_cams.sh
set -euo pipefail

HA_URL="${HA_URL:-http://10.69.42.11:8123}"
TOKEN="${HA_TOKEN:-$(security find-generic-password -a "$USER" -s home-assistant-token -w)}"
WS_URL="${HA_URL/http/ws}/api/websocket"  # http://->ws://, https://->wss://

# The 20 live wyzeapi cameras (camera.<key>). camera.backyard_2 (a GW_WC Sense
# gateway mis-modeled as a camera) is deliberately excluded. The matching stills
# camera.wyze_<key>_snapshot are left visible.
LIVE_KEYS=(
  front_door garden catio studio shop toolbox roundabout cat_flap 3d_printer
  garden_meadow greenhouse_north outside_studio tammy tbd back_yard_cam
  sprouting_shed_1 sprouting_shed_2 garden_pan greenhouse back_greenhouse
)

# ws_rpc: read newline-delimited command JSON on stdin, run one authenticated
# WS session, emit the server's response JSON lines on stdout. The trailing
# sleep keeps stdin open so responses arrive before EOF triggers the close;
# timeout is the safety net.
ws_rpc() {
  local cmds
  cmds="$(cat)"
  {
    printf '%s\n' "{\"type\":\"auth\",\"access_token\":\"$TOKEN\"}"
    printf '%s\n' "$cmds"
    sleep 3
  } | timeout 25 websocat -B 67108864 "$WS_URL" 2>/dev/null
}

echo "Reading entity registry from $WS_URL ..."
list_json="$(printf '%s' '{"id":1,"type":"config/entity_registry/list"}' | ws_rpc)"

registered="$(jq -r 'select(.id==1) | .result[].entity_id' <<<"$list_json" 2>/dev/null)"
if [[ -z "$registered" ]]; then
  echo "ERROR: empty entity registry response (auth or connectivity problem)" >&2
  exit 1
fi

# Decide which keys still need hiding.
to_hide=()
skipped=0 missing=0
for key in "${LIVE_KEYS[@]}"; do
  entity="camera.${key}"
  if ! grep -qx "$entity" <<<"$registered"; then
    echo "MISS   $entity (not in registry)"
    missing=$((missing + 1))
    continue
  fi
  current="$(jq -r --arg e "$entity" \
    'select(.id==1) | .result[] | select(.entity_id==$e) | .hidden_by // "null"' \
    <<<"$list_json")"
  if [[ "$current" == "user" ]]; then
    echo "skip   $entity (already hidden)"
    skipped=$((skipped + 1))
  else
    to_hide+=("$key")
  fi
done

hidden=0 failed=0
if [[ ${#to_hide[@]} -gt 0 ]]; then
  echo "Hiding ${#to_hide[@]} live cameras ..."
  # Build one update command per entity (ids start at 2; id 1 is the list).
  updates="$(
    id=1
    for key in "${to_hide[@]}"; do
      id=$((id + 1))
      jq -nc --argjson id "$id" --arg e "camera.${key}" \
        '{id:$id, type:"config/entity_registry/update", entity_id:$e, hidden_by:"user"}'
    done
  )"
  result_json="$(printf '%s\n' "$updates" | ws_rpc)"

  id=1
  for key in "${to_hide[@]}"; do
    id=$((id + 1))
    entity="camera.${key}"
    ok="$(jq -r --argjson id "$id" \
      'select(.id==$id) | .success' <<<"$result_json" 2>/dev/null)"
    if [[ "$ok" == "true" ]]; then
      echo "hide   $entity"
      hidden=$((hidden + 1))
    else
      err="$(jq -c --argjson id "$id" \
        'select(.id==$id) | .error // .' <<<"$result_json" 2>/dev/null)"
      echo "FAIL   $entity: ${err:-no response}"
      failed=$((failed + 1))
    fi
  done
fi

echo "done: hidden=$hidden skipped=$skipped missing=$missing failed=$failed"
[[ "$failed" -eq 0 && "$missing" -eq 0 ]]
