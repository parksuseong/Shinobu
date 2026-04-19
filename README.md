# Shinobu Project

Shinobu는 KIS(Open API) 기반 5분봉 자동매매 운영 프로젝트입니다.  
Codex로 개발을 이어가기 쉽도록 하네스/운영 명령을 표준화했습니다.

## 핵심 구성

- UI: `app.py` (Streamlit)
- 트레이딩 엔진: `shinobu/live_trading.py`
- 차트/마커: `shinobu/chart_payload.py`, `shinobu/chart_worker.py`, `shinobu/chart_controller.py`
- Signal API: `scripts/run_signal_api.py`, `shinobu/signal_api.py`
- SQLite 캐시 DB: `.streamlit/shinobu_cache.db`

## 빠른 시작 (로컬)

### 1) 가상환경 + 설치

```bash
python -m venv .venv

# Linux/macOS
source .venv/bin/activate

# Windows PowerShell
# .venv\Scripts\Activate.ps1

pip install -U pip
pip install -r requirements.txt
```

### 2) 앱 실행

```bash
python -m streamlit run app.py --server.address 0.0.0.0 --server.port 8501
```

- UI: `http://127.0.0.1:8501`

### 3) Signal API 실행

```bash
python scripts/run_signal_api.py
```

- Base: `http://127.0.0.1:8766`
- Swagger: `http://127.0.0.1:8766/docs`
- Redoc: `http://127.0.0.1:8766/redoc`

## EC2 운영 명령어

서비스 운영은 `scripts/ec2_service.sh` 기준으로 통일합니다.

### 1) 최초 1회

```bash
bash scripts/ec2_service.sh bootstrap
```

### 2) 일반 운영

```bash
bash scripts/ec2_service.sh start
bash scripts/ec2_service.sh stop
bash scripts/ec2_service.sh restart
bash scripts/ec2_service.sh status
```

### 3) 데이터 리셋 + 재수집/재계산 + 자동 기동

```bash
bash scripts/ec2_service.sh reset
```

`reset` 동작:

1. Streamlit/Signal API 중지  
2. sqlite 캐시 데이터 초기화  
3. startup 초기화 플래그 리셋  
4. Streamlit/Signal API 재기동  
5. 앱 시작 후 초기화 스레드에서 캔들 재수집/전략 재계산

## 하네스 (팀 인수인계용)

Codex 표준 검증 루프:

```bash
python scripts/codex_smoke.py
python scripts/codex_report.py
python harness.py
```

상세 가이드:

- `HARNESS.md`
- `AGENTS.md`
- `docs/harness/MEMORY.md`
- `docs/harness/RULES.md`
- `docs/harness/PLAN.md`

## Swagger 주요 엔드포인트

- `GET /health`
- `GET /v1/signals`
- `GET /v1/executions/recent`

예시:

```bash
curl "http://127.0.0.1:8766/v1/signals?from_ts=2026-04-16T09:00:00&to_ts=2026-04-16T15:30:00&sort=desc"
```

## 필수 시크릿

`.streamlit/secrets.toml` 예시:

```toml
KIS_APP_KEY = "..."
KIS_APP_SECRET = "..."
KIS_CANO = "12345678"
KIS_ACNT_PRDT_CD = "01"
KIS_IS_REAL = "true"
```

## 주의사항

- 실매매 계좌에 주문이 나가는 프로젝트입니다.
- 운영 전 반드시 `status`/로그/계좌 상태를 확인하세요.
- EC2 외부에서 Swagger를 보려면 보안그룹에 `8766/tcp` 인바운드 허용이 필요합니다.
