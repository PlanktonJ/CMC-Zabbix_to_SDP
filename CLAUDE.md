# Zabbix → SDP CMDB Sync

## Project
Syncs Zabbix hosts into ServiceDesk Plus CMDB (module: `cmdb_server_new`).
Zabbix is the single source of truth.

## Environment
- Zabbix URL: `http://172.28.236.10:8080/api_jsonrpc.php`
- Auth: Zabbix API token (Bearer) — stored in `config.ini` (gitignored)
- SDP module: `cmdb_server_new`
- SDP auth: `technician_key` header

## Key Files
| File | Purpose |
|---|---|
| `zabbix_to_sdp_4.py` | Main full sync script |
| `test_name_cpu.py` | Test script — interactive group picker, read-only |
| `test_name_cpu.postman_collection.json` | Postman manual test collection |
| `config.ini` | Credentials (gitignored — create locally) |

## config.ini format
```ini
[zabbix]
url      = http://YOUR_ZABBIX_URL/api_jsonrpc.php
username = YOUR_ZABBIX_USER
password = YOUR_ZABBIX_PASSWORD

[sdp]
url     = http://YOUR_SDP_URL/api/v3/ci
api_key = YOUR_SDP_API_KEY

[sync]
log_file  = zabbix_to_sdp.log
log_level = INFO
```

## Field Mapping
| SDP Field | Source | Notes |
|---|---|---|
| `name` | `host["host"]` | Zabbix technical hostname |
| `udf_hostname` | `host["host"]` | Match key for upsert |
| `udf_cpu` | `system.cpu.num[snmp]` → fallback `system.cpu.num` | Most devices are SNMP |
| `udf_vram` | `vm.memory.size[total]` → GB int | |
| `udf_cmdb_4019` | `vfs.fs.size[/,total]` → GB label e.g. `100G` | |
| `udf_cmdb_4018` | `system.uname` parsed | OS version string |
| `udf_cmdb_3974` | `system.uname` parsed | Linux / Windows / Unix |
| `udf_t_nh_tr_ng_gi_m_s_t` | `host["status"]` | `{"id": "2204"}` monitored / `{"id": "2206"}` not |
| `udf_cmdb_4014` | group name keywords | `{"id": "2223"}` virtual / `{"id": "2222"}` physical |
| `udf_vm_platform` | group name keywords | `{"id": "2217"}` cloud / `{"id": "2501"}` physical |

## SDP API Quirks
- Payloads sent as form field `input_data` (JSON string), not JSON body
- PUT updates only send Zabbix-owned fields — SDP-owned fields are never touched
- Match key: `udf_hostname` (Zabbix `host["host"]`)

## Run Modes
```bash
# Full sync
python3 zabbix_to_sdp_4.py

# Dry run
python3 zabbix_to_sdp_4.py --dry-run --limit 3

# Sync one group interactively
python3 zabbix_to_sdp_4.py --group-sync

# Test: name + udf_cpu only (read-only, interactive group picker)
python3 test_name_cpu.py
python3 test_name_cpu.py --json
```

## SNMP CPU Key
Most devices monitored via SNMP — CPU item key is `system.cpu.num[snmp]`.
Fallback for agent-monitored hosts: `system.cpu.num`.
