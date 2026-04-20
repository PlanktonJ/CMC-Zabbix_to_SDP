#!/usr/bin/env python3
"""
zabbix_to_sdp.py
Syncs Zabbix hosts into ServiceDesk Plus CMDB (module: cmdb_server_new).

Zabbix is the single source of truth:
  - Only fields Zabbix owns are written to SDP.
  - Fields maintained by the SDP team are never touched.
  - Hosts removed from Zabbix are deactivated in SDP (inactive=true).
  - Match key: udf_hostname (Zabbix technical hostname = host["host"]).

Usage:
  python3 zabbix_to_sdp.py                      # full sync
  python3 zabbix_to_sdp.py --dry-run            # print payloads, no SDP writes
  python3 zabbix_to_sdp.py --dry-run --limit 3  # dry-run first 3 hosts
  python3 zabbix_to_sdp.py --limit 1            # live run on first host only
"""

import argparse
import configparser
import json
import logging
import re
import sys
from pathlib import Path

import requests

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────

CONFIG_FILE = Path(__file__).parent / "config.ini"

config = configparser.ConfigParser()
config.read(CONFIG_FILE)

ZABBIX_URL  = config["zabbix"]["url"]
ZABBIX_USER = config["zabbix"]["username"]
ZABBIX_PASS = config["zabbix"]["password"]

SDP_URL     = config["sdp"]["url"]
SDP_API_KEY = config["sdp"]["api_key"]

LOG_FILE    = config["sync"]["log_file"]
LOG_LEVEL   = config["sync"]["log_level"].upper()

# SDP module — fixed
SDP_MODULE = "cmdb_server_new"

# ──────────────────────────────────────────────
# SDP OBJECT FIELD IDs
# Fields that require {"id": "..."} instead of a plain string.
# Source: your live CMDB data.
# ──────────────────────────────────────────────

MONITORING_STATUS_ID = {
    "monitored":     "2204",   # Da giam sat
    "not_monitored": "2206",   # Khong giam sat
}

VM_PLATFORM_ID = {
    "physical": "2501",   # Vat ly
    "cloud":    "2217",   # Cloudv2 — extend if you have VMware/KVM IDs
}

PHYS_VIRT_ID = {
    "physical": "2222",   # Vat Ly
    "virtual":  "2223",   # Ao hoa
}

# ──────────────────────────────────────────────
# ZABBIX ITEM KEYS
# Verify these exist on your hosts:
#   Zabbix UI -> Monitoring -> Latest Data -> filter by host
# ──────────────────────────────────────────────

ITEM_KEYS = [
    "vm.memory.size[total]",  # Total RAM bytes -> GB integer string
    "vfs.fs.size[/,total]",   # Total root disk bytes -> GB label e.g. "100G"
    "system.cpu.num",         # CPU core count
    "system.uname",           # Full OS/kernel string
]

# ──────────────────────────────────────────────
# FIELD OWNERSHIP
#
# ZABBIX_OWNED: written on every sync — Zabbix is authoritative.
# SDP_OWNED:    never written by this script — SDP team manages these.
#
# Any udf_field NOT listed in ZABBIX_OWNED is treated as SDP-owned.
# ──────────────────────────────────────────────

ZABBIX_OWNED_UDF = {
    "udf_hostname",
    "udf_ip_private",
    "udf_ip_gi_m_s_t",
    "udf_cpu",
    "udf_cmdb_3998",       # CPU type: vCPU / Physical CPU
    "udf_vram",
    "udf_ram_model",
    "udf_cmdb_4019",       # Disk total
    "udf_cmdb_3974",       # OS type: Linux / Windows / Unix
    "udf_cmdb_4018",       # OS version string
    "udf_t_nh_tr_ng_gi_m_s_t",  # Monitoring status (object)
    "udf_cmdb_4014",       # Physical / Virtual (object)
    "udf_vm_platform",     # VM platform (object)
}

# Top-level fields Zabbix owns (outside udf_fields)
ZABBIX_OWNED_TOP = {"name", "description", "inactive"}

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# ZABBIX API CLIENT
# ──────────────────────────────────────────────

class ZabbixAPI:
    """Minimal Zabbix JSON-RPC 2.0 client."""

    def __init__(self, url: str):
        self.url     = url
        self.auth    = None
        self.req_id  = 1
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def _call(self, method: str, params) -> any:
        payload = {
            "jsonrpc": "2.0",
            "method":  method,
            "params":  params,
            "auth":    self.auth,
            "id":      self.req_id,
        }
        self.req_id += 1
        resp = self.session.post(self.url, json=payload, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        if "error" in body:
            raise RuntimeError(f"Zabbix API error [{method}]: {body['error']}")
        return body["result"]

    def login(self, user: str, password: str) -> None:
        self.auth = self._call("user.login", {"user": user, "password": password})
        log.debug("Zabbix login OK.")

    def logout(self) -> None:
        try:
            self._call("user.logout", [])
        except Exception:
            pass
        self.auth = None
        log.debug("Zabbix logout OK.")

    def get_hosts(self) -> list[dict]:
        """
        Fetch all hosts with interfaces and host groups.
        host["host"]  = technical hostname  (match key, maps to udf_hostname)
        host["name"]  = display name        (maps to SDP top-level 'name')
        host["status"]= "0" monitored / "1" not monitored
        """
        return self._call("host.get", {
            "output":           ["hostid", "host", "name", "status", "description"],
            "selectInterfaces": ["ip", "dns", "type", "main", "useip"],
            "selectGroups":     ["name"],
        })

    def get_host_groups(self) -> list[dict]:
        """Return all host groups that contain at least one host."""
        return self._call("hostgroup.get", {
            "output":      ["groupid", "name"],
            "real_hosts":  True,
            "sortfield":   "name",
        })

    def get_hosts_by_group(self, group_id: str) -> list[dict]:
        """Fetch hosts belonging to a specific host group."""
        return self._call("host.get", {
            "output":           ["hostid", "host", "name", "status", "description"],
            "selectInterfaces": ["ip", "dns", "type", "main", "useip"],
            "selectGroups":     ["name"],
            "groupids":         [group_id],
        })

    def get_items(self, host_ids: list[str]) -> list[dict]:
        """Fetch all matching item keys for the given host IDs in one call."""
        return self._call("item.get", {
            "output":  ["hostid", "name", "key_", "lastvalue", "units"],
            "hostids": host_ids,
            "filter":  {"key_": ITEM_KEYS},
        })


# ──────────────────────────────────────────────
# TRANSFORMATION HELPERS
# ──────────────────────────────────────────────

def bytes_to_gb_int(value_str: str) -> str:
    """
    Bytes -> whole GB integer string.
    udf_vram format: "8", "16", "32"  (no unit suffix).
    """
    try:
        gb = int(value_str) // (1024 ** 3)
        return str(gb) if gb > 0 else "N/A"
    except (ValueError, TypeError):
        return "N/A"


def bytes_to_gb_label(value_str: str) -> str:
    """
    Bytes -> GB label string.
    udf_cmdb_4019 format: "100G", "480G", "960G".
    """
    try:
        gb = int(value_str) // (1024 ** 3)
        return f"{gb}G" if gb > 0 else "N/A"
    except (ValueError, TypeError):
        return "N/A"


def parse_os_type(uname: str) -> str:
    """
    udf_cmdb_3974: "Linux" | "Windows" | "Unix" | "N/A"
    Derived from system.uname e.g. "Linux host 5.15.0 #1 SMP GNU/Linux"
    """
    if not uname:
        return "N/A"
    u = uname.lower()
    if "linux"   in u: return "Linux"
    if "windows" in u: return "Windows"
    if any(k in u for k in ("unix", "aix", "solaris", "bsd")): return "Unix"
    return "N/A"


def parse_os_version(uname: str) -> str:
    """
    udf_cmdb_4018: human-readable OS version.
    Examples from your CMDB: "Ubuntu server 20.04", "Pfsense 2.7.2"
    Extend patterns to match OS variants in your environment.
    """
    if not uname:
        return "N/A"
    patterns = [
        (r"ubuntu[^\d]*(\d+\.\d+)",                        lambda m: f"Ubuntu {m.group(1)}"),
        (r"(red hat|centos|rhel)[^\d]*(\d+[\.\d]*)",       lambda m: f"{m.group(1).title()} {m.group(2)}"),
        (r"windows server (\d+)",                          lambda m: f"Windows Server {m.group(1)}"),
        (r"debian[^\d]*(\d+)",                             lambda m: f"Debian {m.group(1)}"),
        (r"rocky[^\d]*(\d+[\.\d]*)",                       lambda m: f"Rocky Linux {m.group(1)}"),
        (r"alma[^\d]*(\d+[\.\d]*)",                        lambda m: f"AlmaLinux {m.group(1)}"),
    ]
    for pattern, formatter in patterns:
        m = re.search(pattern, uname, re.IGNORECASE)
        if m:
            return formatter(m)
    return uname[:50]   # Fallback: first 50 chars of raw uname


def derive_virtual(groups: list[dict]) -> bool:
    """
    Detect virtual hosts from Zabbix group names.
    Adjust keywords to match your Zabbix group naming convention.
    """
    virtual_keywords = {"virtual", "vm", "cloud", "vmware", "kvm", "hyper-v", "xen"}
    return any(
        any(k in g["name"].lower() for k in virtual_keywords)
        for g in groups
    )


def get_primary_interface(interfaces: list[dict]) -> str:
    """
    Return the IP of the main/default interface.
    main="1" is the default interface in Zabbix.
    Falls back to the first interface if none is marked main.
    """
    primary = next((i for i in interfaces if i.get("main") == "1"), None)
    if primary is None and interfaces:
        primary = interfaces[0]
    return primary.get("ip") or "N/A" if primary else "N/A"


def build_host_records(hosts: list[dict], items: list[dict]) -> list[dict]:
    """
    Join Zabbix host data + item data into normalised records.
    Each record contains only values Zabbix knows about.
    """
    # Index items: hostid -> {key_: item}
    items_by_host: dict[str, dict] = {}
    for item in items:
        items_by_host.setdefault(item["hostid"], {})[item["key_"]] = item

    records = []
    for host in hosts:
        hid     = host["hostid"]
        h_items = items_by_host.get(hid, {})
        groups  = host.get("groups", [])

        is_virtual   = derive_virtual(groups)
        is_monitored = str(host.get("status", "1")) == "0"
        ip           = get_primary_interface(host.get("interfaces", []))

        ram_raw  = h_items.get("vm.memory.size[total]", {}).get("lastvalue", "")
        disk_raw = h_items.get("vfs.fs.size[/,total]",  {}).get("lastvalue", "")
        cpu_raw  = h_items.get("system.cpu.num",        {}).get("lastvalue", "")
        uname    = h_items.get("system.uname",          {}).get("lastvalue", "")

        records.append({
            # ── Match key (never changes) ──
            "hostname":      host["host"],          # Zabbix technical name = udf_hostname

            # ── Zabbix-owned top-level SDP fields ──
            "name":          host["name"],          # Zabbix display name -> SDP 'name'
            "description":   host.get("description") or "",
            "is_inactive":   not is_monitored,      # Zabbix disabled -> SDP inactive=true

            # ── Zabbix-owned udf fields ──
            "ip_private":    ip,
            "ip_monitoring": ip,
            "cpu_count":     cpu_raw if cpu_raw else "N/A",
            "cpu_type":      "vCPU" if is_virtual else "Physical CPU",
            "ram_gb":        bytes_to_gb_int(ram_raw),
            "ram_model":     "vRAM" if is_virtual else "Physical RAM",
            "disk_total":    bytes_to_gb_label(disk_raw),
            "os_type":       parse_os_type(uname),
            "os_version":    parse_os_version(uname),
            "is_virtual":    is_virtual,
            "is_monitored":  is_monitored,
        })

    return records


# ──────────────────────────────────────────────
# SDP CMDB API CLIENT
# ──────────────────────────────────────────────

class SDPAPI:
    """
    ServiceDesk Plus CMDB REST client — module: cmdb_server_new.

    SDP quirk: payloads go as form field 'input_data' (JSON string),
    not as a JSON request body.

    Zabbix is the single source of truth:
      _build_payload()  → only Zabbix-owned fields
      _deactivate_payload() → sets inactive=true, touches nothing else
    """

    def __init__(self, base_url: str, api_key: str):
        self.base    = base_url.rstrip("/")
        self.api_key = api_key
        self.session = requests.Session()

    def _headers(self) -> dict:
        return {"technician_key": self.api_key}

    def _wrap(self, payload: dict) -> dict:
        return {"input_data": json.dumps(payload)}

    def _build_payload(self, record: dict) -> dict:
        """
        Build SDP payload containing ONLY Zabbix-owned fields.
        SDP-owned fields (rack, site, warranty, etc.) are intentionally absent
        so SDP will not overwrite them on update.
        """
        monitoring_id  = MONITORING_STATUS_ID["monitored" if record["is_monitored"] else "not_monitored"]
        vm_platform_id = VM_PLATFORM_ID["cloud" if record["is_virtual"] else "physical"]
        phys_virt_id   = PHYS_VIRT_ID["virtual" if record["is_virtual"] else "physical"]

        return {
            # Zabbix-owned top-level fields only
            "name":        record["name"],
            "description": record["description"],
            "inactive":    record["is_inactive"],

            # Zabbix-owned udf fields only — SDP fields not listed here are untouched
            "udf_fields": {
                "udf_hostname":            record["hostname"],
                "udf_ip_private":          record["ip_private"],
                "udf_ip_gi_m_s_t":         record["ip_monitoring"],
                "udf_cpu":                 record["cpu_count"],
                "udf_cmdb_3998":           record["cpu_type"],
                "udf_vram":                record["ram_gb"],
                "udf_ram_model":           record["ram_model"],
                "udf_cmdb_4019":           record["disk_total"],
                "udf_cmdb_3974":           record["os_type"],
                "udf_cmdb_4018":           record["os_version"],
                "udf_t_nh_tr_ng_gi_m_s_t": {"id": monitoring_id},
                "udf_cmdb_4014":           {"id": phys_virt_id},
                "udf_vm_platform":         {"id": vm_platform_id},
            }
        }

    def _deactivate_payload(self) -> dict:
        """
        Minimal payload to deactivate a CI that no longer exists in Zabbix.
        Only sets inactive=true — every other field is left exactly as-is in SDP.
        """
        return {"inactive": True}

    # ── Search ──

    def search_by_hostname(self, hostname: str) -> str | None:
        """
        Search cmdb_server_new by udf_hostname (Zabbix technical hostname).
        Returns SDP CI ID string if found, None if not found.

        If this returns empty results unexpectedly, try changing
        "field": "udf_hostname" to "field": "udf_fields.udf_hostname"
        — SDP version determines which path syntax it accepts.
        """
        payload = {
            "list_info": {
                "row_count":   1,
                "start_index": 1,
                "search_criteria": [
                    {"field": "udf_hostname", "condition": "is", "value": hostname}
                ],
            }
        }
        resp = self.session.get(
            f"{self.base}/{SDP_MODULE}",
            headers=self._headers(),
            params=self._wrap(payload),
            timeout=30,
        )
        resp.raise_for_status()
        ci_list = resp.json().get(SDP_MODULE, [])
        return str(ci_list[0]["id"]) if ci_list else None

    def get_all_ci_hostnames(self) -> dict[str, str]:
        """
        Fetch ALL CI records from SDP and return a dict of:
            {udf_hostname -> ci_id}

        Used to detect CIs in SDP that no longer exist in Zabbix
        so they can be deactivated.

        Paginates automatically until has_more_rows = false.
        """
        result   = {}
        start    = 1
        row_count = 100  # fetch 100 per page

        while True:
            payload = {
                "list_info": {
                    "row_count":   row_count,
                    "start_index": start,
                }
            }
            resp = self.session.get(
                f"{self.base}/{SDP_MODULE}",
                headers=self._headers(),
                params=self._wrap(payload),
                timeout=30,
            )
            resp.raise_for_status()
            data    = resp.json()
            ci_list = data.get(SDP_MODULE, [])

            for ci in ci_list:
                hostname = ci.get("udf_fields", {}).get("udf_hostname")
                ci_id    = str(ci["id"])
                if hostname:
                    result[hostname] = ci_id

            has_more = data.get("list_info", {}).get("has_more_rows", False)
            if not has_more:
                break
            start += row_count

        return result

    # ── Write ──

    def create_ci(self, record: dict) -> dict:
        resp = self.session.post(
            f"{self.base}/{SDP_MODULE}",
            headers=self._headers(),
            data=self._wrap(self._build_payload(record)),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def update_ci(self, ci_id: str, record: dict) -> dict:
        resp = self.session.put(
            f"{self.base}/{SDP_MODULE}/{ci_id}",
            headers=self._headers(),
            data=self._wrap(self._build_payload(record)),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def deactivate_ci(self, ci_id: str) -> dict:
        """Set inactive=true on a CI that no longer exists in Zabbix."""
        resp = self.session.put(
            f"{self.base}/{SDP_MODULE}/{ci_id}",
            headers=self._headers(),
            data=self._wrap(self._deactivate_payload()),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    # ── Upsert (match on udf_hostname) ──

    def upsert_ci(self, record: dict) -> tuple[str, str]:
        """
        Match on udf_hostname.
        Found  -> update Zabbix-owned fields only.
        Not found -> create new CI.
        Returns (action, ci_id).
        """
        existing_id = self.search_by_hostname(record["hostname"])
        if existing_id:
            self.update_ci(existing_id, record)
            return "updated", existing_id
        else:
            result = self.create_ci(record)
            new_id = str(result.get(SDP_MODULE, {}).get("id", "unknown"))
            return "created", new_id


# ──────────────────────────────────────────────
# INTERACTIVE GROUP SELECTION
# ──────────────────────────────────────────────

def select_group_interactive(groups: list[dict]) -> dict:
    """
    Print numbered list of Zabbix host groups and prompt the user to pick one.
    Returns the selected group dict {"groupid": ..., "name": ...}.
    """
    print("\nAvailable Zabbix host groups:")
    for i, g in enumerate(groups, 1):
        print(f"  {i:3}. {g['name']}  (id={g['groupid']})")
    while True:
        raw = input(f"\nSelect group [1-{len(groups)}]: ").strip()
        if raw.isdigit() and 1 <= int(raw) <= len(groups):
            return groups[int(raw) - 1]
        print(f"  Please enter a number between 1 and {len(groups)}.")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Sync Zabbix hosts -> SDP CMDB (cmdb_server_new). "
            "Zabbix is the single source of truth."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Pull from Zabbix, print payloads. No writes to SDP.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="Process only the first N Zabbix hosts. 0 = all (default).",
    )
    parser.add_argument(
        "--skip-deactivate",
        action="store_true",
        help="Skip the deactivation pass for CIs removed from Zabbix.",
    )
    parser.add_argument(
        "--group-sync",
        action="store_true",
        help="Interactively select a Zabbix host group and sync only its hosts.",
    )
    args = parser.parse_args()

    log.info("=" * 65)
    if args.dry_run:
        log.info("*** DRY RUN — no writes will be made to SDP ***")
    log.info("Zabbix -> SDP CMDB sync started. Zabbix = single source of truth.")

    # ── 1. Pull all hosts + items from Zabbix ──
    zabbix = ZabbixAPI(ZABBIX_URL)
    try:
        zabbix.login(ZABBIX_USER, ZABBIX_PASS)

        if args.group_sync:
            log.info("Fetching host groups from Zabbix...")
            groups = zabbix.get_host_groups()
            log.info(f"  {len(groups)} host groups found.")
            selected = select_group_interactive(groups)
            log.info(f"Selected group: {selected['name']}  (id={selected['groupid']})")

            log.info("Fetching hosts for selected group...")
            hosts = zabbix.get_hosts_by_group(selected["groupid"])
            log.info(f"  {len(hosts)} hosts found in group.")
        else:
            log.info("Fetching hosts from Zabbix...")
            hosts = zabbix.get_hosts()
            log.info(f"  {len(hosts)} hosts found.")

        host_ids = [h["hostid"] for h in hosts]

        log.info("Fetching items from Zabbix...")
        items = zabbix.get_items(host_ids)
        log.info(f"  {len(items)} matching items found.")
    finally:
        zabbix.logout()

    # ── 2. Transform ──
    records = build_host_records(hosts, items)

    # Build a set of Zabbix hostnames for the deactivation pass later
    zabbix_hostnames = {r["hostname"] for r in records}

    if args.limit > 0:
        records = records[:args.limit]
        log.info(f"Limiting to first {args.limit} host(s).")

    log.info(f"Built {len(records)} records for sync.")

    sdp   = SDPAPI(SDP_URL, SDP_API_KEY)
    stats = {"created": 0, "updated": 0, "deactivated": 0, "failed": 0}

    # ── 3. Dry run: print and exit ──
    if args.dry_run:
        log.info("-" * 65)
        for record in records:
            payload = sdp._build_payload(record)
            log.info(f"[DRY-RUN] {record['hostname']}  ({record['name']})")
            print(json.dumps(payload, indent=2, ensure_ascii=False))
            print()
        log.info("Dry run complete. No changes made.")
        log.info("=" * 65)
        sys.exit(0)

    # ── 4. Upsert: Zabbix hosts -> SDP ──
    log.info("-" * 65)
    log.info("Pass 1/2 — Upserting Zabbix hosts into SDP...")

    for record in records:
        try:
            action, ci_id = sdp.upsert_ci(record)
            stats[action] += 1
            log.info(
                f"  [{action.upper():7}] {record['hostname']}"
                f"  name={record['name']}  id={ci_id}"
            )
        except requests.HTTPError as e:
            stats["failed"] += 1
            log.error(
                f"  [FAILED ] {record['hostname']}: "
                f"HTTP {e.response.status_code} — {e.response.text[:300]}"
            )
        except Exception as e:
            stats["failed"] += 1
            log.error(f"  [FAILED ] {record['hostname']}: {e}")

    # ── 5. Deactivation pass: SDP hosts not in Zabbix ──
    if args.skip_deactivate:
        log.info("Pass 2/2 — Deactivation skipped (--skip-deactivate).")
    elif args.limit > 0 or args.group_sync:
        log.info("Pass 2/2 — Deactivation skipped (subset run).")
    else:
        log.info("Pass 2/2 — Checking for SDP CIs no longer in Zabbix...")
        try:
            sdp_hostnames = sdp.get_all_ci_hostnames()
            log.info(f"  {len(sdp_hostnames)} CIs found in SDP.")

            stale = {
                hostname: ci_id
                for hostname, ci_id in sdp_hostnames.items()
                if hostname not in zabbix_hostnames
            }
            log.info(f"  {len(stale)} stale CIs to deactivate.")

            for hostname, ci_id in stale.items():
                try:
                    sdp.deactivate_ci(ci_id)
                    stats["deactivated"] += 1
                    log.info(f"  [DEACTIVATED] {hostname}  (id={ci_id})")
                except requests.HTTPError as e:
                    stats["failed"] += 1
                    log.error(
                        f"  [FAILED ] deactivate {hostname}: "
                        f"HTTP {e.response.status_code} — {e.response.text[:300]}"
                    )
                except Exception as e:
                    stats["failed"] += 1
                    log.error(f"  [FAILED ] deactivate {hostname}: {e}")

        except Exception as e:
            log.error(f"  Deactivation pass failed: {e}")

    # ── 6. Summary ──
    log.info("=" * 65)
    log.info(
        f"Sync complete.  "
        f"Created: {stats['created']}  "
        f"Updated: {stats['updated']}  "
        f"Deactivated: {stats['deactivated']}  "
        f"Failed: {stats['failed']}"
    )
    log.info("=" * 65)

    if stats["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
