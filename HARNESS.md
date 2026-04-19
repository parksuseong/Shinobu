# Shinobu Harness

시노부 프로젝트를 Codex로 이어서 개발할 때 쓰는 표준 하네스 문서입니다.

## 문서 읽기 순서

1. `AGENTS.md`
2. `docs/harness/MEMORY.md`
3. `docs/harness/RULES.md`
4. `docs/harness/PLAN.md`
5. `HARNESS.md`

## 표준 검증 루프

아래 3개를 항상 순서대로 실행합니다.

```bash
python scripts/codex_smoke.py
python scripts/codex_report.py
python harness.py
```

## 로컬 실행 커맨드

```bash
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows PowerShell
# .venv\Scripts\Activate.ps1

pip install -U pip
pip install -r requirements.txt
python -m streamlit run app.py --server.address 0.0.0.0 --server.port 8501
```

Signal API:

```bash
python scripts/run_signal_api.py
```

- Swagger: `http://127.0.0.1:8766/docs`

## EC2 운영 커맨드

```bash
bash scripts/ec2_service.sh bootstrap
bash scripts/ec2_service.sh start
bash scripts/ec2_service.sh stop
bash scripts/ec2_service.sh restart
bash scripts/ec2_service.sh status
```

데이터 리셋 + 재수집/재계산 + 재기동:

```bash
bash scripts/ec2_service.sh reset
```

## 장애 대응 빠른 확인

Signal API 죽었을 때:

```bash
tail -n 120 .streamlit/signal_api.err.log
lsof -i :8766 -P -n
```

포트 충돌이면 점유 PID 종료 후 재시작:

```bash
kill -9 <PID>
bash scripts/ec2_service.sh start
```
