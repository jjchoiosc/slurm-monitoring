# Slurm Monitor — Portable HA Package

Slurm client 명령으로 클러스터 상태를 조회하는 Python 웹 대시보드입니다.
추가 Python 패키지 없이 Slurm client가 설치된 Linux 호스트에 배포할 수 있습니다.

## 현재 HA 구성

```text
Browser
  ├─ http://192.168.31.179:8120  (Slurm monitor VIP)
  └─ http://192.168.31.150:8165  (bm-80 HAProxy)
                         │
                         ▼
             keepalived VIP 192.168.31.179
                ┌────────┴────────┐
                │                 │
       slurm1 / MASTER     slurm2 / BACKUP
       192.168.31.177      192.168.31.178
       monitor :8120       monitor :8120
```

| Host | Role | Address |
|------|------|---------|
| slurm1 | Slurm primary + monitor MASTER | `192.168.31.177` |
| slurm2 | Slurm backup + monitor BACKUP | `192.168.31.178` |
| keepalived | Monitor VIP | `192.168.31.179:8120` |
| bm-80 | HAProxy portal endpoint | `192.168.31.150:8165` |

HAProxy와 keepalived는 `/api/health`를 확인합니다. 이 endpoint는 웹 프로세스뿐 아니라
`sinfo`가 Slurm controller를 정상 조회하는지도 검사합니다.

## 기능

- `sinfo`, `squeue`, `scontrol` 대시보드 (30초 자동 갱신)
- DOWN, DRAIN, Not responding node 경고
- `sdiag`, `sprio`, `sshare`, `sstat`, `sacct`, `sreport` 등 조회
- 변경성 명령은 help-only로 제한
- REST API 및 정적 웹 UI
- systemd 자동 재시작
- keepalived active/standby VIP

## 디렉터리

```text
slurm-monitoring/
├── server.py
├── static/index.html
├── slurm-monitor.service
├── .env.example
├── scripts/check-slurm-monitor   # keepalived health check
├── deploy/keepalived-monitor.conf.example
├── MIGRATION.md
└── README.md
```

## 요구사항

- Linux + systemd
- Python 3
- Slurm client (`sinfo`, `squeue`, `scontrol`)
- root 또는 passwordless sudo
- HA 설치 시 `apt`와 keepalived
- 실행 사용자가 `/etc/slurm/slurm.conf`을 읽을 수 있어야 함

서비스는 기본적으로 root로 실행되므로 현재 `slurm:slurm 750` 구성에서도 동작합니다.

## 현재 클러스터에 HA 배포

작업 PC에서:

```bash
cd slurm-monitor
chmod +x ./*.sh
SSH_USER=nova \
PRIMARY_HOST=192.168.31.177 \
BACKUP_HOST=192.168.31.178 \
HA_VIP=192.168.31.179 \
./deploy-ha.sh
```

각 노드에서 직접 설치할 수도 있습니다.

```bash
# slurm1
sudo HA_ROLE=MASTER \
  HA_PEER_IP=192.168.31.178 \
  HA_VIP=192.168.31.179 \
  ./install-ha.sh

# slurm2
sudo HA_ROLE=BACKUP \
  HA_PEER_IP=192.168.31.177 \
  HA_VIP=192.168.31.179 \
  ./install-ha.sh
```

## 다른 환경에 단일 노드 설치

```bash
tar -xzf slurm-monitor-YYYYMMDD.tar.gz
cd slurm-monitor
sudo SLURM_MONITOR_PORT=8120 \
  SLURM_MONITOR_CLUSTER_NAME=my-cluster \
  ./install.sh
```

설정은 `/etc/default/slurm-monitor`에 저장됩니다.

```text
SLURM_MONITOR_HOST=0.0.0.0
SLURM_MONITOR_PORT=8120
SLURM_MONITOR_TIMEOUT=30
SLURM_MONITOR_CLUSTER_NAME=my-cluster
SLURM_PRIMARY_IP=192.168.31.177
SLURM_BACKUP_IP=192.168.31.178
```

## 다른 환경에 HA 설치

```bash
sudo HA_ROLE=MASTER \
  HA_PEER_IP=<backup-ip> \
  HA_VIP=<monitor-vip> \
  HA_INTERFACE=<interface> \
  SLURM_MONITOR_CLUSTER_NAME=<cluster-name> \
  ./install-ha.sh
```

지원 환경변수:

| 변수 | 기본값 |
|------|--------|
| `HA_ROLE` | 필수 (`MASTER` 또는 `BACKUP`) |
| `HA_PEER_IP` | 필수 |
| `HA_VIP` | `192.168.31.179` |
| `HA_INTERFACE` | default route 인터페이스 |
| `HA_VRID` | `79` |
| `SLURM_MONITOR_PORT` | `8120` |
| `SLURM_MONITOR_CLUSTER_NAME` | `supernova` |
| `INSTALL_DIR` | `/opt/slurm-monitor` |

## Portable package 생성

```bash
./package.sh
```

결과:

```text
dist/slurm-monitor-YYYYMMDD.tar.gz
```

버전과 출력 파일 지정:

```bash
VERSION=1.0.0 ./package.sh /tmp/slurm-monitor-1.0.0.tar.gz
```

## bm-80 HAProxy

```bash
scp configure-haproxy.sh jjchoi@192.168.31.80:/tmp/
ssh jjchoi@192.168.31.80 \
  'chmod +x /tmp/configure-haproxy.sh && /tmp/configure-haproxy.sh'
```

기본 backend는 `192.168.31.179:8120`입니다.

## 확인

```bash
curl http://192.168.31.179:8120/api/health
curl http://192.168.31.179:8120/api/dashboard
curl http://192.168.31.150:8165/api/health

ssh nova@192.168.31.177 'ip addr show eth0; systemctl status keepalived slurm-monitor'
ssh nova@192.168.31.178 'ip addr show eth0; systemctl status keepalived slurm-monitor'
```

## 서비스 관리

```bash
sudo systemctl restart slurm-monitor
sudo systemctl restart keepalived
sudo journalctl -u slurm-monitor -f
sudo journalctl -u keepalived -f
```

## 접속

- Monitor VIP: http://192.168.31.179:8120
- Daboja/HAProxy: http://192.168.31.150:8165
- Daboja portal: http://192.168.31.150:8160
