#!/usr/bin/env bash
#
# Provision the Wyze still-image cameras as Home Assistant local_file config
# entries.
#
# WHY A SCRIPT (not a package): in HA 2026.5.x the `local_file` camera platform
# no longer supports YAML/package setup ("does not support platform setup")
# -- it is config-entry-only. Config entries live in the root-owned
# /config/.storage/core.config_entries (not in the repo), so this idempotent
# script is the version-controlled source of truth for creating them.
#
# Each entry shows a still JPEG written by the wyze-snapshot sidecar
# (/config/wyze_snapshots/<key>.jpg). The entity_id is derived by HA from the
# entry name: "Wyze <Title> Snapshot" -> camera.wyze_<key>_snapshot. The
# Cameras dashboard (cameras-dashboard.yaml) renders these stills and taps
# through to the live WebRTC camera.<key>.
#
# Idempotent: existing camera.wyze_<key>_snapshot entities are skipped, so it is
# safe to re-run. Requires an ADMIN token (config flow is admin-only); read from
# the macOS Keychain, never hardcoded.
#
# Usage: ./provision_local_file_cameras.sh
set -euo pipefail

HA_URL="${HA_URL:-http://10.69.42.11:8123}"
TOKEN="${HA_TOKEN:-$(security find-generic-password -a "$USER" -s home-assistant-token -w)}"
SNAP_DIR="${SNAP_DIR:-/config/wyze_snapshots}"

# key (= jpg filename stem = live camera.<key>) | Title (slugifies back to key)
ENTRIES=(
  "front_door|Front Door"
  "garden|Garden"
  "catio|Catio"
  "studio|Studio"
  "shop|Shop"
  "toolbox|Toolbox"
  "roundabout|Roundabout"
  "cat_flap|Cat Flap"
  "3d_printer|3D Printer"
  "garden_meadow|Garden Meadow"
  "greenhouse_north|Greenhouse North"
  "outside_studio|Outside Studio"
  "tammy|Tammy"
  "tbd|TBD"
  "back_yard_cam|Back Yard Cam"
  "sprouting_shed_1|Sprouting Shed 1"
  "sprouting_shed_2|Sprouting Shed 2"
  "garden_pan|Garden Pan"
  "greenhouse|Greenhouse"
  "back_greenhouse|Back Greenhouse"
)

api() { curl -fsS -m 20 -H "Authorization: Bearer $TOKEN" "$@"; }

# Snapshot of existing entities for the idempotency check.
existing="$(api "$HA_URL/api/states" | jq -r '.[].entity_id')"

created=0 skipped=0 failed=0
for row in "${ENTRIES[@]}"; do
  key="${row%%|*}"
  title="${row#*|}"
  entity="camera.wyze_${key}_snapshot"
  name="Wyze ${title} Snapshot"
  path="${SNAP_DIR}/${key}.jpg"

  if grep -qx "$entity" <<<"$existing"; then
    echo "skip   $entity (exists)"
    skipped=$((skipped + 1))
    continue
  fi

  flow_id="$(api -X POST -H 'Content-Type: application/json' \
    -d '{"handler":"local_file","show_advanced_options":false}' \
    "$HA_URL/api/config/config_entries/flow" | jq -r '.flow_id')"

  result="$(api -X POST -H 'Content-Type: application/json' \
    -d "$(jq -n --arg n "$name" --arg p "$path" '{name:$n,file_path:$p}')" \
    "$HA_URL/api/config/config_entries/flow/$flow_id")"

  if [[ "$(jq -r '.type' <<<"$result")" == "create_entry" ]]; then
    echo "create $entity -> $path"
    created=$((created + 1))
  else
    echo "FAIL   $entity: $(jq -c '.errors // .reason // .' <<<"$result")"
    failed=$((failed + 1))
  fi
done

echo "done: created=$created skipped=$skipped failed=$failed"
[[ "$failed" -eq 0 ]]
