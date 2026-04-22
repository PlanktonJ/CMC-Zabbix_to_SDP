#!/usr/bin/env python3
"""
sync_name_cpu.py
Pulls name + udf_cpu + OS + memory + IP + hostname from Zabbix for a selected group,
upserts into SDP CMDB.

Flow:
  STEP 1 — fetch host groups, prompt user to pick one
  STEP 2 — fetch all hosts in that group
  STEP 3 — fetch CPU / OS / memory / hostname items for all hosts
  STEP 4 — upsert to SDP: match on name -> update fields
            if not found in SDP -> create new CI

Usage:
  python3 sync_name_cpu.py               # interactive, live upsert
  python3 sync_name_cpu.py --dry-run     # print payloads, no SDP writes
"""

import argparse
import datetime
import json
import logging
import sys
from pathlib import Path

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ──────────────────────────────────────────────
# CONFIG  — fill in before running
# ──────────────────────────────────────────────

ZABBIX_URL   = "http://YOUR_ZABBIX_HOST/api_jsonrpc.php"
ZABBIX_TOKEN = "YOUR_ZABBIX_API_TOKEN"

SDP_URL     = "https://YOUR_SDP_HOST/api/v3"
SDP_API_KEY = "YOUR_SDP_API_KEY"
SDP_MODULE  = "cmdb_nb_noc_sysapi"

# CPU item keys — SNMP_PRIORITY_KEY is preferred when both exist on a host
ITEM_KEY_CPU_SNMP    = ["system.cpu.num", "system.cpu.num[snmp]"]
SNMP_PRIORITY_KEY    = "system.cpu.num[snmp]"   # used for priority comparison

ITEM_KEY_OS          = "net.if.osversion"
ITEM_SEARCH_MEMORY   = ["total memory"]
ITEM_KEY_HOSTNAME    = "system.name"

# Zabbix host status → SDP monitoring label
STATUS_MAP = {
    "0": "Đã giám sát",      # enabled  (monitored)
    "1": "Không giám sát",   # disabled (not monitored)
}

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────

_run_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_DIR  = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"sync_name_cpu_{_run_ts}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# ZABBIX API CLIENT
# ──────────────────────────────────────────────

class ZabbixAPI:
    """Minimal Zabbix JSON-RPC 2.0 client — API token auth."""

    def __init__(self, url: str, token: str):
        self.url     = url
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

    def get_groups(self) -> list[dict]:
        return self._call("hostgroup.get", {
            "output":     ["groupid", "name"],
            "real_hosts": True,
            "sortfield":  "name",
        })

    def get_hosts_by_group(self, group_id: str) -> list[dict]:
        """Returns hosts with embedded interfaces (IP already included) and status."""
        return self._call("host.get", {
            "output":           ["hostid", "host", "status"],
            "groupids":         [group_id],
            "sortfield":        "host",
            "selectInterfaces": ["ip"],
        })

    def get_cpu(self, host_ids: list[str]) -> list[dict]:
        return self._call("item.get", {
            "output":  ["hostid", "key_", "lastvalue"],
            "hostids": host_ids,
            "filter":  {"key_": ITEM_KEY_CPU_SNMP},
        })

    def get_os(self, host_ids: list[str]) -> list[dict]:
        return self._call("item.get", {
            "output":  ["hostid", "key_", "lastvalue"],
            "hostids": host_ids,
            "filter":  {"key_": [ITEM_KEY_OS]},
        })

    def get_mem(self, host_ids: list[str]) -> list[dict]:
        return self._call("item.get", {
            "output":  ["hostid", "name", "lastvalue"],
            "hostids": host_ids,
            "search":  {"name": ITEM_SEARCH_MEMORY},
        })

    def get_hostname(self, host_ids: list[str]) -> list[dict]:
        return self._call("item.get", {
            "output":  ["hostid", "key_", "lastvalue"],
            "hostids": host_ids,
            "filter":  {"key_": ITEM_KEY_HOSTNAME},
        })


# ──────────────────────────────────────────────
# SDP CMDB CLIENT
# ──────────────────────────────────────────────

class SDPAPI:
    """SDP CMDB client — upsert CI with name + udf_fields."""

    def __init__(self, base_url: str, api_key: str):
        self.base    = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.verify = False   # internal server — self-signed cert
        self.headers = {"technician_key": api_key}

    def _wrap(self, payload: dict) -> dict:
        return {"input_data": json.dumps(payload)}

    def search_by_hostname(self, hostname: str) -> str | None:
        """Search by name, return CI id or None."""
        payload = {
            "list_info": {
                "row_count":   1,
                "start_index": 1,
                "search_criteria": [
                    {"field": "name", "condition": "is", "value": hostname}
                ],
            }
        }
        resp = self.session.get(
            f"{self.base}/{SDP_MODULE}",
            headers=self.headers,
            params=self._wrap(payload),
            timeout=30,
        )
        resp.raise_for_status()
        ci_list = resp.json().get(SDP_MODULE, [])
        return str(ci_list[0]["id"]) if ci_list else None

    def _ci_payload(self, record: dict) -> dict:
        return {
            SDP_MODULE: {
                "name": record["name"],
                "udf_fields": {
                    "udf_cpu":                  record["udf_cpu"],
                    "udf_ip_private":           record["udf_ip"],
                    "udf_ip_gi_m_s_t":          record["udf_ip"],
                    "udf_cmdb_4018":            record["udf_os"],
                    "udf_vram":                 record["udf_mem"],
                    "udf_hostname":             record["udf_hostname"],
                    "udf_t_nh_tr_ng_gi_m_s_t": {"name": record["udf_status"]},
                },
            }
        }

    def update_ci(self, ci_id: str, record: dict) -> dict:
        resp = self.session.put(
            f"{self.base}/{SDP_MODULE}/{ci_id}",
            headers=self.headers,
            data=self._wrap(self._ci_payload(record)),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def create_ci(self, record: dict) -> str:
        resp = self.session.post(
            f"{self.base}/{SDP_MODULE}",
            headers=self.headers,
            data=self._wrap(self._ci_payload(record)),
            timeout=30,
        )
        resp.raise_for_status()
        return str(resp.json().get(SDP_MODULE, {}).get("id", "?"))


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
# HELPERS
# ──────────────────────────────────────────────

def bytes_to_gb(value: str) -> str:
    """Convert Zabbix lastvalue (bytes string) to rounded GB string, e.g. '16.00 GB'."""
    try:
        gb = int(value) / (1024 ** 3)
        return f"{gb:.2f} GB"
    except (ValueError, TypeError):
        return "N/A"


# ──────────────────────────────────────────────
# BUILD RECORDS
# ──────────────────────────────────────────────

def build_records(
    hosts: list[dict],
    cpu_items: list[dict],
    os_items: list[dict],
    mem_items: list[dict],
    hostname_items: list[dict],
) -> list[dict]:
    # CPU: SNMP_PRIORITY_KEY takes priority over agent key
    cpu_by_host: dict[str, str] = {}
    for item in cpu_items:
        hid = item["hostid"]
        if item["key_"] == SNMP_PRIORITY_KEY or hid not in cpu_by_host:
            cpu_by_host[hid] = item["lastvalue"]

    os_by_host: dict[str, str] = {}
    for item in os_items:
        hid = item["hostid"]
        os_by_host[hid] = item["lastvalue"]

    mem_by_host: dict[str, str] = {}
    for item in mem_items:
        hid = item["hostid"]
        mem_by_host[hid] = item["lastvalue"]

    hostname_by_host: dict[str, str] = {}
    for item in hostname_items:
        hid = item["hostid"]
        hostname_by_host[hid] = item["lastvalue"]

    return [
        {
            "hostid":      h["hostid"],
            "name":        h["host"],
            "udf_cpu":     cpu_by_host.get(h["hostid"], "") or "N/A",
            "udf_ip":      h.get("interfaces", [{}])[0].get("ip", "N/A"),
            "udf_os":      os_by_host.get(h["hostid"], "") or "N/A",
            "udf_mem":     bytes_to_gb(mem_by_host.get(h["hostid"], "")),
            "udf_hostname": hostname_by_host.get(h["hostid"], "") or "N/A",
            "udf_status":  STATUS_MAP.get(str(h.get("status", "1")), "Không giám sát"),
        }
        for h in hosts
    ]


# ──────────────────────────────────────────────
# STEP 4 — upsert to SDP
# ──────────────────────────────────────────────

def upsert_to_sdp(records: list[dict], sdp: SDPAPI, dry_run: bool) -> None:
    stats = {"created": 0, "updated": 0, "failed": 0}

    col = max((len(r["name"]) for r in records), default=30)
    col = max(col, 30)

    print()
    print(f"  {'name':<{col}}  {'udf_cpu':>8}  result")
    print("  " + "-" * (col + 22))

    for record in records:
        hostname = record["name"]

        if dry_run:
            payload = {
                "name": record["name"],
                "udf_fields": {
                    "udf_cpu":      record["udf_cpu"],
                    "udf_ip":       record["udf_ip"],
                    "udf_os":       record["udf_os"],
                    "udf_mem":      record["udf_mem"],
                    "udf_hostname": record["udf_hostname"],
                },
            }
            print(f"  {hostname:<{col}}  {record['udf_cpu']:>8}  [DRY-RUN] {json.dumps(payload)}")
            continue

        try:
            ci_id = sdp.search_by_hostname(hostname)

            if ci_id is None:
                new_id = sdp.create_ci(record)
                stats["created"] += 1
                print(f"  {hostname:<{col}}  {record['udf_cpu']:>8}  [CREATED] ci_id={new_id}")
            else:
                sdp.update_ci(ci_id, record)
                stats["updated"] += 1
                print(f"  {hostname:<{col}}  {record['udf_cpu']:>8}  [UPDATED] ci_id={ci_id}")

        except requests.HTTPError as e:
            stats["failed"] += 1
            msg = f"HTTP {e.response.status_code} — {e.response.text[:200]}"
            print(f"  {hostname:<{col}}  {record['udf_cpu']:>8}  [FAILED] {msg}")
            log.error(f"[FAILED] {hostname}: {msg}")
        except Exception as e:
            stats["failed"] += 1
            print(f"  {hostname:<{col}}  {record['udf_cpu']:>8}  [FAILED] {e}")
            log.exception(f"[FAILED] {hostname}")

    print()
    if not dry_run:
        log.info(f"Created: {stats['created']}  |  Updated: {stats['updated']}  |  Failed: {stats['failed']}")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Sync name + CPU + OS + mem + IP + hostname from Zabbix group -> SDP CMDB. Match key: name."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print payloads only. No writes to SDP.")
    args = parser.parse_args()

    log.info("=" * 55)
    if args.dry_run:
        log.info("DRY RUN — no writes to SDP.")
    log.info("Sync: name | CPU | OS | mem | IP | hostname -> SDP CMDB")
    log.info("=" * 55)

    zabbix = ZabbixAPI(ZABBIX_URL, ZABBIX_TOKEN)
    sdp    = SDPAPI(SDP_URL, SDP_API_KEY)

    # ── STEP 1 ──
    log.info("STEP 1 — Fetching host groups...")
    groups = zabbix.get_groups()
    log.info(f"  {len(groups)} groups found.")
    chosen_group = prompt_group(groups)

    # ── STEP 2 ──
    log.info(f"STEP 2 — Fetching hosts in '{chosen_group['name']}'...")
    hosts = zabbix.get_hosts_by_group(chosen_group["groupid"])
    log.info(f"  {len(hosts)} hosts found.")

    if not hosts:
        log.warning("No hosts in this group. Exiting.")
        sys.exit(0)

    # ── STEP 3 ──
    host_ids = [h["hostid"] for h in hosts]
    log.info(f"STEP 3 — Fetching items for {len(host_ids)} hosts...")
    cpu_items      = zabbix.get_cpu(host_ids)
    mem_items      = zabbix.get_mem(host_ids)
    os_items       = zabbix.get_os(host_ids)
    hostname_items = zabbix.get_hostname(host_ids)
    log.info(f"  CPU items: {len(cpu_items)}  |  OS items: {len(os_items)}  |  Mem items: {len(mem_items)}")

    records = build_records(hosts, cpu_items, os_items, mem_items, hostname_items)
    no_cpu  = sum(1 for r in records if r["udf_cpu"] == "N/A")
    log.info(f"  With CPU: {len(records) - no_cpu}/{len(records)}  |  Missing CPU: {no_cpu}")

    # ── STEP 4 ──
    log.info(f"STEP 4 — Upserting {len(records)} records to SDP...")
    log.info("=" * 55)
    upsert_to_sdp(records, sdp, dry_run=args.dry_run)
    log.info("=" * 55)

    if args.dry_run:
        log.info("Dry run complete. No changes made to SDP.")


if __name__ == "__main__":
    main()
