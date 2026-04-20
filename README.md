# Shinobu

Shinobu는 `buy`와 `sell` 전략을 기반으로 동작하는 자동매매 프로그램입니다.

## 개요

이 프로젝트의 목표는 다음과 같습니다.

- 시장 데이터를 수집한다.
- 매수(`buy`) 조건을 판단한다.
- 매도(`sell`) 조건을 판단한다.
- 주문을 실행하고 결과를 기록한다.
- 추후 전략 고도화와 리스크 관리 기능을 확장한다.

## 핵심 방향

- 자동화: 조건 충족 시 사람이 직접 개입하지 않아도 주문이 실행되도록 설계
- 안정성: 주문 전 검증, 예외 처리, 로그 기록을 우선 고려
- 확장성: 전략, 거래소 연동, 알림 기능을 모듈 단위로 확장 가능하도록 구성

## 예상 기능

- 실시간 또는 주기적 시세 조회
- 매수/매도 신호 계산
- 거래소 API 연동
- 주문 실행 및 체결 상태 확인
- 손절/익절 규칙 적용
- 거래 로그 저장
- 백테스트 및 전략 검증

## 기본 구조 예시

향후 프로젝트는 아래와 같은 흐름으로 확장할 수 있습니다.

1. 데이터 수집
2. 전략 분석
3. `buy` 또는 `sell` 신호 생성
4. 주문 실행
5. 결과 기록 및 리스크 점검

## 개발 메모

- 현재는 프로젝트 초기 세팅 단계입니다.
- 먼저 기본 실행 구조를 만들고, 이후 거래소 API와 전략 로직을 연결하는 방향으로 진행합니다.
- 민감한 정보(API Key, Secret)는 코드에 직접 넣지 않고 환경 변수 또는 별도 설정 파일로 관리합니다.

## 실행 예시

```bash
python main.py
```

## 저장소

GitHub repository:

- [parksuseong/Shinobu](https://github.com/parksuseong/Shinobu)

## Codex Harness

Use the harness loop for quick verification while editing:

```bash
python scripts/codex_smoke.py
python scripts/codex_report.py
python harness.py
```

See `HARNESS.md` for details.

## One-Command Release (AWS CLI)

1. Copy `.env.release.example` to `.env.release`.
2. Fill required values:
   - `GIT_REMOTE_URL`
   - `AWS_PROFILE`
   - `AWS_REGION`
   - `EC2_HOST`
   - `EC2_SSH_KEY_PATH`
   - `EC2_APP_DIR`
   - `EC2_DEPLOY_COMMAND`
3. Run one-time AWS setup:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup_aws_cli.ps1 -Profile default -Region ap-northeast-2
```

4. Run release:

```bash
python scripts/release.py -m "chore: release"
```

Default deploy command uses `scripts/deploy_ec2.ps1`.
