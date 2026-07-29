# Slurm Monitor 다른 환경으로 이전하기

이 문서는 Slurm Monitor package를 새로운 Slurm 클러스터로 이전하는 절차입니다.
단일 노드와 keepalived HA 구성을 모두 설명합니다.

## 1. 이전 구성 결정

먼저 새 환경의 값을 정합니다.

```text
Cluster name:
Primary controller IP:
Backup controller IP:       # 단일 구성에서는 없음
Monitor VIP:                # 단일 구성에서는 없음
Network interface:
Monitor port:               # 기본 8120
SSH user:
```

HA 구성 예시:

```text
Cluster name: new-cluster
Primary controller IP: 10.10.10.11
Backup controller IP: 10.10.10.12
Monitor VIP: 10.10.10.13
Network interface: eth0
Monitor port: 8120
SSH user: ubuntu
```

VIP는 primary/backup과 같은 subnet에서 사용하지 않는 주소여야 합니다.
아래 명령으로 중복 여부를 확인합니다.

```bash
ping -c 3 <monitor-vip>
arping -D -I <interface> <monitor-vip>
```

응답이 있다면 이미 사용 중일 수 있으므로 다른 주소를 선택합니다.

## 2. 요구사항 확인

두 controller에서 확인합니다.

```bash
hostname
ip -br addr
python3 --version
command -v sinfo squeue scontrol
sudo -n true
sudo scontrol ping
sudo sinfo
```

필수 조건:

- Linux와 systemd
- Python 3
- Slurm client 명령
- 정상적인 `/etc/slurm/slurm.conf`
- root 또는 passwordless sudo
- HA 구성 시 두 노드 간 VRRP/unicast 통신
- monitor port에 대한 방화벽 허용

두 Slurm controller가 모두 표시되는지 확인합니다.

```bash
sudo scontrol ping
```

정상 예:

```text
Slurmctld(primary) at slurm1 is UP
Slurmctld(backup) at slurm2 is UP
```

## 3. package 복사

기존 서버에서 archive를 생성합니다.

```bash
cd slurm-monitor
chmod +x ./*.sh
./package.sh
```

생성 파일:

```text
dist/slurm-monitor-YYYYMMDD.tar.gz
```

새 환경의 작업 PC 또는 primary controller로 복사합니다.

```bash
scp dist/slurm-monitor-YYYYMMDD.tar.gz \
  <ssh-user>@<primary-ip>:/tmp/
```

압축을 풉니다.

```bash
ssh <ssh-user>@<primary-ip>
mkdir -p ~/slurm-monitor
tar -xzf /tmp/slurm-monitor-YYYYMMDD.tar.gz -C ~/slurm-monitor
cd ~/slurm-monitor
chmod +x ./*.sh
```

## 4-A. 단일 노드 설치

HA가 필요하지 않다면 Slurm controller 또는 Slurm client 한 대에 설치합니다.

```bash
sudo \
  SLURM_MONITOR_CLUSTER_NAME=<cluster-name> \
  SLURM_MONITOR_PORT=8120 \
  SLURM_PRIMARY_IP=<controller-ip> \
  ./install.sh
```

방화벽을 사용하는 경우 monitor port를 허용합니다.

```bash
sudo ufw allow 8120/tcp
```

확인:

```bash
systemctl status slurm-monitor --no-pager
curl http://127.0.0.1:8120/api/health
curl http://<controller-ip>:8120/api/controller
```

접속 URL:

```text
http://<controller-ip>:8120
```

## 4-B. HA 환경 설치

작업 PC에서 package 디렉터리를 준비한 후 다음 명령을 실행합니다.

```bash
SSH_USER=<ssh-user> \
PRIMARY_HOST=<primary-ip> \
BACKUP_HOST=<backup-ip> \
HA_VIP=<monitor-vip> \
SLURM_MONITOR_PORT=8120 \
./deploy-ha.sh
```

`deploy-ha.sh`는 다음 작업을 수행합니다.

1. package를 primary와 backup의 `/tmp/slurm-monitor-package`로 복사
2. 양쪽에 Slurm Monitor systemd service 설치
3. keepalived 설치
4. primary를 priority 150의 MASTER로 구성
5. backup을 priority 100의 BACKUP으로 구성
6. `/api/health` 기반 VIP 상태 검사 설정

SSH 원격 배포를 사용할 수 없다면 각 노드에서 직접 설치합니다.

Primary:

```bash
sudo \
  HA_ROLE=MASTER \
  HA_PEER_IP=<backup-ip> \
  HA_VIP=<monitor-vip> \
  HA_INTERFACE=<interface> \
  SLURM_MONITOR_CLUSTER_NAME=<cluster-name> \
  ./install-ha.sh
```

Backup:

```bash
sudo \
  HA_ROLE=BACKUP \
  HA_PEER_IP=<primary-ip> \
  HA_VIP=<monitor-vip> \
  HA_INTERFACE=<interface> \
  SLURM_MONITOR_CLUSTER_NAME=<cluster-name> \
  ./install-ha.sh
```

## 5. 방화벽 확인

두 노드에서 monitor port를 허용합니다.

```bash
sudo ufw allow 8120/tcp
```

keepalived는 unicast VRRP를 사용합니다. 방화벽 정책이 엄격하면 두 controller IP
사이에 IP protocol 112를 허용해야 합니다.

```bash
sudo ufw allow from <peer-ip> to any proto vrrp
```

사용 중인 UFW 버전이 `proto vrrp`를 지원하지 않으면 nftables/iptables에서
IP protocol 112를 허용합니다.

## 6. 설치 상태 확인

Primary:

```bash
ssh <ssh-user>@<primary-ip> \
  'systemctl is-active slurmctld slurm-monitor keepalived; ip -4 -br addr'
```

Backup:

```bash
ssh <ssh-user>@<backup-ip> \
  'systemctl is-active slurmctld slurm-monitor keepalived; ip -4 -br addr'
```

정상 상태에서는 VIP가 primary에만 있어야 합니다.

API를 확인합니다.

```bash
curl http://<monitor-vip>:8120/api/health
curl http://<monitor-vip>:8120/api/controller
curl -o /dev/null -w '%{http_code}\n' \
  http://<monitor-vip>:8120/api/dashboard
```

`/api/controller` 정상 예:

```json
{
  "active": {
    "host": "slurm1",
    "ip": "10.10.10.11",
    "status": "UP"
  },
  "reachable": true
}
```

웹 브라우저에서 다음을 확인합니다.

```text
http://<monitor-vip>:8120
```

- 왼쪽 `CURRENT MASTER` 아이콘에 올바른 controller 표시
- `sinfo`, `squeue`, `scontrol` 결과 표시
- node warning 상태 표시

## 7. VIP failover 시험

Slurm controller는 중지하지 않고 primary의 keepalived만 잠시 중지합니다.

```bash
ssh <ssh-user>@<primary-ip> 'sudo systemctl stop keepalived'
sleep 8
curl http://<monitor-vip>:8120/api/health
ssh <ssh-user>@<backup-ip> 'ip -4 -br addr'
```

응답 host와 VIP가 backup으로 이동해야 합니다.

시험 후 primary를 복구합니다.

```bash
ssh <ssh-user>@<primary-ip> 'sudo systemctl start keepalived'
sleep 8
curl http://<monitor-vip>:8120/api/health
ssh <ssh-user>@<primary-ip> 'ip -4 -br addr'
```

VIP가 primary로 돌아오면 정상입니다.

## 8. 외부 HAProxy 연결

HAProxy를 사용하는 경우 backend를 monitor VIP로 지정합니다.

```haproxy
frontend slurm-monitor
    bind <haproxy-vip>:8165
    mode http
    default_backend slurm-monitor-http

backend slurm-monitor-http
    mode http
    option httpchk GET /api/health
    http-check expect status 200
    server slurm-ha-vip <monitor-vip>:8120 check
```

적용 전 설정을 검사합니다.

```bash
sudo haproxy -c -f /etc/haproxy/haproxy.cfg
sudo systemctl reload haproxy
curl http://<haproxy-vip>:8165/api/health
```

## 9. 설정 파일 위치

```text
/opt/slurm-monitor/                 application
/etc/default/slurm-monitor          monitor environment
/etc/systemd/system/slurm-monitor.service
/etc/keepalived/keepalived.conf
/etc/default/slurm-monitor-ha
/usr/local/sbin/check-slurm-monitor
```

Monitor 설정 예:

```text
SLURM_MONITOR_HOST=0.0.0.0
SLURM_MONITOR_PORT=8120
SLURM_MONITOR_TIMEOUT=30
SLURM_MONITOR_CLUSTER_NAME=new-cluster
SLURM_PRIMARY_IP=10.10.10.11
SLURM_BACKUP_IP=10.10.10.12
```

설정 변경 후:

```bash
sudo systemctl restart slurm-monitor
sudo systemctl restart keepalived
```

## 10. 문제 해결

### `/api/health`가 HTTP 503인 경우

```bash
sudo sinfo
sudo scontrol ping
sudo journalctl -u slurm-monitor -n 100 --no-pager
```

Slurm 명령이 실패하면 monitor가 아니라 Slurm 설정, DNS, munge 또는 controller
통신 문제를 먼저 해결합니다.

### `Munge decode failed: Invalid credential`

두 controller에서 key checksum과 시간을 비교합니다.

```bash
sudo sha256sum /etc/munge/munge.key
date
```

key 파일을 갱신한 직후라면 daemon이 이전 key를 메모리에 보관하고 있을 수 있습니다.
운영 작업 시간을 확보한 후 munge와 Slurm daemon을 순서대로 재시작합니다.

### VIP가 생성되지 않는 경우

```bash
sudo keepalived --config-test
sudo journalctl -u keepalived -n 100 --no-pager
curl -i http://127.0.0.1:8120/api/health
```

health check가 실패하면 keepalived는 해당 노드를 VIP owner로 선택하지 않습니다.

### UI에 IP가 표시되지 않는 경우

`/etc/default/slurm-monitor`에서 다음 값을 설정하고 서비스를 재시작합니다.

```text
SLURM_PRIMARY_IP=<primary-ip>
SLURM_BACKUP_IP=<backup-ip>
```

## 11. 롤백

새 monitor를 중지합니다.

```bash
sudo systemctl stop keepalived
sudo systemctl stop slurm-monitor
sudo systemctl disable keepalived slurm-monitor
```

외부 HAProxy를 사용했다면 backend를 이전 monitor 주소로 되돌린 후 검증하고
reload합니다.

```bash
sudo haproxy -c -f /etc/haproxy/haproxy.cfg
sudo systemctl reload haproxy
```

Slurm Monitor는 조회 전용 웹 서비스이므로 중지해도 Slurm job 실행에는 영향을 주지
않습니다. 단, `slurmctld`, `slurmd`, `munge` 서비스는 rollback 과정에서 임의로
중지하지 마십시오.
