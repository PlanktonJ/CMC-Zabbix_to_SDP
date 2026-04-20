#!/usr/bin/env python3
"""
test_name_cpu.py
Test script — Zabbix read-only. Zero SDP writes.

Flow:
  STEP 1 — fetch all host groups, prompt user to pick one
  STEP 2 — fetch all hosts in that group
  STEP 3 — fetch system.cpu.num[snmp] / system.cpu.num for all hosts, print table

Usage:
  python3 test_name_cpu.py
  python3 test_name_cpu.py --json     # output as SDP payload fragments
"""

import argparse
import json
import logging
import sys

import requests

# ──────────────────────────────────────────────
# CONFIG  — fill in before running
# ──────────────────────────────────────────────

ZABBIX_URL   = "http://YOUR_ZABBIX_URL/api_jsonrpc.php"
ZABBIX_TOKEN = "YOUR_ZABBIX_API_TOKEN"   # Zabbix 5.4+ API token

ITEM_KEY_CPU_SNMP  = "system.cpu.num[snmp]"
ITEM_KEY_CPU_AGENT = "system.cpu.num"

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# ZABBIX API CLIENT
# ──────────────────────────────────────────────

class ZabbixAPI:
    """Minimal Zabbix JSON-RPC 2.0 client — API token auth."""

    def __init__(self, url: str, token: str):
        self.url     = url
        self.token   = token
        self.req_id  = 1
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {token}",
        })

    def _call(self, method: str, params) -> any:
        payload = {
            "jsonrpc": "2.0",
            "method":  method,
            "params":  params,
            "id":      self.req_id,
        }
        self.req_id += 1
        resp = self.session.post(self.url, json=payload, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        if "error" in body:
            raise RuntimeError(f"Zabbix API error [{method}]: {body['error']}")
        return body["result"]

    # ── STEP 1 ──
    def get_groups(self) -> list[dict]:
        return self._call("hostgroup.get", {
            "output":     ["groupid", "name"],
            "real_hosts": True,
            "sortfield":  "name",
        })

    # ── STEP 2 ──
    def get_hosts_by_group(self, group_id: str) -> list[dict]:
        return self._call("host.get", {
            "output":   ["hostid", "host", "status"],
            "groupids": [group_id],
            "sortfield": "host",
        })

    # ── STEP 3 ──
    def get_cpu_items(self, host_ids: list[str]) -> list[dict]:
        return self._call("item.get", {
            "output":  ["hostid", "key_", "lastvalue"],
            "hostids": host_ids,
            "filter":  {"key_": [ITEM_KEY_CPU_SNMP, ITEM_KEY_CPU_AGENT]},
        })


# ──────────────────────────────────────────────
# STEP 1 — prompt user to pick a group
# ──────────────────────────────────────────────

def prompt_group(groups: list[dict]) -> dict:
    print()
    print("=" * 55)
    print(f"  {'#':>4}  {'groupid':>8}  name")
    print("=" * 55)
    for i, g in enumerate(groups, 1):
        print(f"  {i:>4}  {g['groupid']:>8}  {g['name']}")
    print("=" * 55)

    while True:
        raw = input(f"\nSelect group [1-{len(groups)}]: ").strip()
        if raw.isdigit() and 1 <= int(raw) <= len(groups):
            chosen = groups[int(raw) - 1]
            print(f"  >> Selected: {chosen['name']}  (groupid={chosen['groupid']})\n")
            return chosen
        print(f"  Enter a number between 1 and {len(groups)}.")


# ──────────────────────────────────────────────
# BUILD RECORDS
# ──────────────────────────────────────────────

def build_records(hosts: list[dict], cpu_items: list[dict]) -> list[dict]:
    # SNMP key takes priority; agent key is fallback
    cpu_by_host: dict[str, str] = {}
    for item in cpu_items:
        hid = item["hostid"]
        if item["key_"] == ITEM_KEY_CPU_SNMP or hid not in cpu_by_host:
            cpu_by_host[hid] = item["lastvalue"]

    return [
        {
            "hostid":    h["hostid"],
            "name":      h["host"],                              # -> SDP name
            "udf_cpu":   cpu_by_host.get(h["hostid"], "") or "N/A",  # -> SDP udf_cpu
            "monitored": h.get("status", "1") == "0",
        }
        for h in hosts
    ]


# ──────────────────────────────────────────────
# OUTPUT
# ──────────────────────────────────────────────

def print_table(records: list[dict]) -> None:
    if not records:
        print("  (no hosts)")
        return
    col = max((len(r["name"]) for r in records), default=30)
    col = max(col, 30)

    header = f"  {'hostid':>10}  {'name':<{col}}  {'udf_cpu':>8}  status"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for r in records:
        cpu     = r["udf_cpu"] if r["udf_cpu"] != "N/A" else "-- N/A --"
        status  = "monitored" if r["monitored"] else "disabled"
        print(f"  {r['hostid']:>10}  {r['name']:<{col}}  {cpu:>8}  {status}")
    print()


def print_sdp_payloads(records: list[dict]) -> None:
    out = [
        {"name": r["name"], "udf_fields": {"udf_cpu": r["udf_cpu"]}}
        for r in records
    ]
    print(json.dumps(out, indent=2, ensure_ascii=False))


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Test Zabbix -> SDP field mapping. Read-only, no SDP writes."
    )
    parser.add_argument("--json", dest="as_json", action="store_true",
                        help="Print SDP payload JSON instead of table.")
    args = parser.parse_args()

    log.info("=" * 55)
    log.info("TEST MODE — Zabbix read-only. Zero SDP writes.")
    log.info("=" * 55)

    zabbix = ZabbixAPI(ZABBIX_URL, ZABBIX_TOKEN)

    # ── STEP 1: get groups, prompt choice ──
    log.info("STEP 1 — Fetching host groups...")
    groups = zabbix.get_groups()
    log.info(f"  {len(groups)} groups found.")

    chosen_group = prompt_group(groups)

    # ── STEP 2: get hosts in chosen group ──
    log.info(f"STEP 2 — Fetching hosts in '{chosen_group['name']}'...")
    hosts = zabbix.get_hosts_by_group(chosen_group["groupid"])
    log.info(f"  {len(hosts)} hosts found.")

    if not hosts:
        log.warning("No hosts in this group. Exiting.")
        sys.exit(0)

    host_ids = [h["hostid"] for h in hosts]

    # ── STEP 3: get CPU items for all hosts ──
    log.info(f"STEP 3 — Fetching CPU items for {len(host_ids)} hosts...")
    cpu_items = zabbix.get_cpu_items(host_ids)
    log.info(f"  {len(cpu_items)} CPU items returned.")

    records = build_records(hosts, cpu_items)

    no_cpu = sum(1 for r in records if r["udf_cpu"] == "N/A")
    log.info(f"  With CPU data: {len(records) - no_cpu}/{len(records)}  |  Missing: {no_cpu}")
    log.info("=" * 55)

    if args.as_json:
        print_sdp_payloads(records)
    else:
        print(f"\nGroup: {chosen_group['name']}  (groupid={chosen_group['groupid']})")
        print_table(records)


if __name__ == "__main__":
    main()
