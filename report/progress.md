# Tiến trình thực hiện

## CP0 — Repo, danh tính, tài nguyên và trạng thái ban đầu

- Thời điểm: 2026-10-12 15:42–15:45 (Asia/Saigon).
- Repo/worktree: nhánh `codex/cv-data-pipeline-execution`, baseline `ef2ab58d356464127f65a9e529904af30d852eff`; origin đúng repo `hongcuong26-debug/K4-L3L4-Track4-DataPipeline4ComputerVision`.
- Danh tính xác nhận: Nguyễn Hồng Cường, MSSV `2A202602415`, GitHub `hongcuong26-debug`.
- Không có `AGENTS.md`. Plan đã được chép vào `docs/superpowers/plans/2026-10-12-cv-data-pipeline-plan.md`.
- Docker Desktop 4.67.0 / Engine 29.3.1 / Compose v5.1.1 đã chạy; không có container thuộc project `day09` từ checkout khác.
- Docker chỉ có `3797110784` byte RAM (~3.54 GiB), thấp hơn yêu cầu 6 GiB; sẽ chạy profile orchestrate trước, giữ Metabase tắt đến CP8 và ghi lỗi thật nếu OOM.
- Ổ C còn 21.55 GiB, vượt yêu cầu khoảng 8 GiB.
- Host không có `python`, `python3` hoặc `py` trong PATH; các lệnh Python sẽ chạy trong container Airflow khi phù hợp. Không sửa script để né môi trường.
- Blob Git của `tests/test_units.py`, `pipeline/verify.py`, `docker-compose.yml` khớp SHA256 trong `scripts/protected.json`. File checkout có CRLF do Git for Windows `core.autocrlf=true`, nên hash byte của working tree lệch dù `git diff` rỗng; cần bảo đảm checker chạy trên nội dung LF chuẩn mà không sửa logic đề.
- `dataset/day09-data.zip` đang được track và còn local (27,791,983 byte); CP1 sẽ giải nén, kiểm manifest, rồi bỏ ZIP khỏi index nhưng giữ file local.
- Bằng chứng: `report/logs/cp0_preflight.txt`.
- Bước tiếp theo: tạo `.env` với MSSV đã xác nhận, chuẩn bị dữ liệu, bật stack và chạy smoke CP1.

## CP1 — Chuẩn bị dữ liệu và smoke run toàn pipeline

- `.env` local giữ đúng `LAB_STUDENT_ID=2A202602415`; container Airflow in lại đúng cùng chuỗi.
- ZIP chuẩn có SHA256 `3f6e8a36c0e4ac0b6c9ca3124abd9b9f4008acacb97f177a672c0551bda85542`; `check_data.py` nguyên bản xác nhận 6/6 file đúng manifest.
- `dataset/day09-data.zip` đã bỏ khỏi Git index nhưng còn local; không có video/CSV/sources/DB/tar bị track (giữ `data/MANIFEST.json`, `data/README.md`, `work/.gitkeep`).
- Lần build đầu kéo image và tạo `day09-airflow`; MinIO, Postgres, Airflow đều healthy. `/health` báo metadatabase và scheduler healthy.
- Phát hiện lỗi môi trường Windows: `core.autocrlf=true` biến shebang thành `#!/bin/bash\r`, khiến Airflow thoát 255 với `exec /airflow-entrypoint.sh: no such file or directory`. Đã dùng `dos2unix` chỉ để chuẩn hóa byte LF trong worktree; Git diff mã nguồn rỗng, hash ba file bảo vệ khớp lại. Rebuild xác nhận Airflow chạy và scheduler heartbeat bình thường.
- Git Bash cần `MSYS_NO_PATHCONV=1` để không đổi `-w /opt/lab` thành đường dẫn Windows; đây là biến tạm của host, không sửa `scripts/lab.sh`.
- Smoke `scripts/lab.sh step all --reset` đạt `27/27 checks passed`; `report/evidence/run_summary.json.verify_all_passed=true`.
- Kết quả smoke tạm: version `b2fb840da0d5`, 1.019 Gold frame; train/val/test = 693/164/162; 26 trip = 18/4/4.
- Bằng chứng: `report/logs/cp1_data_preflight.txt`, `cp1_compose_up.txt`, `cp1_health.txt`, `smoke_all.txt`, `report/evidence/run_summary.json`.
- Bước tiếp theo: CP2 chạy lại riêng Bronze, đọc history/video_files và điền Câu 1 bằng quan sát thật.

## CP2 — Bước 1: Bronze và schema drift

- `scripts/lab.sh step 1 --reset` xóa 2.343 object cũ, nạp 3.021 dòng telemetry A, 3.234 dòng B, 26 trip và 2 video; module 1 đạt 5/5.
- Lượt ingest lặp trong lệnh chỉ chạy lại drive A và báo video/telemetry `already present`; không diễn giải thành cả hai drive được ingest hai lần.
- Delta history của `bronze.raw_telemetry_a`: đúng một WRITE ở version 0, `rows_added=3021`.
- `bronze.video_files`: drive A offset 0 và SHA256 `636ba1a98dfc...`; drive B offset 1.500 ms và SHA256 `bfa62703f645...`.
- MinIO read-only listing xác nhận `bronze/video/drive_a/raw.mp4`, `bronze/video/drive_b/raw.mp4` và bốn prefix Delta dưới `bronze/delta/`.
- Câu 1 đã điền bằng số liệu từ `report/logs/step1.txt`, `bronze_history.txt`, `bronze_video_files.txt`, `bronze_objects.txt`.
- Bước tiếp theo: CP3 chạy Silver telemetry, rejects và thí nghiệm break-gate.

## CP3 — Bước 2: Silver telemetry, quarantine và break-gate

- `scripts/lab.sh step 2`: dbt run PASS=6; dbt test PASS=6/WARN=1/ERROR=0; module 2 đạt 7/7.
- Silver có 6.149 dòng sạch và 106 rejects. Mỗi drive: duplicate 20, speed_missing 30, speed_out_of_range 3; clean + rejects khớp raw.
- B được đổi km/h÷3,6, epoch ms→UTC và trừ offset 1.500 ms; telemetry start `14:30:00.009` so với video `14:30:00.000`.
- `max_gap_ms` severity warn; `in_range` mặc định error theo `dbt/models/schema.yml`.
- `break_gate.json` thuộc MSSV `2A202602415`, `gate_failed=true`; test in_range bắt 6 dòng ở pha nới. Pha phục hồi `PASS=6 WARN=1 ERROR=0`.
- Câu 2–3 đã điền từ `step2.txt`, `rejects.txt`, `telemetry_clock.txt`, `break_gate.txt` và JSON thí nghiệm.
- Bước tiếp theo: CP4 chạy frames, 9 unit tests, idempotency và threshold.

## CP4 — Bước 3: Frames, unit test, idempotency và threshold

- Xử lý 26 trip thành 1.246 frame; dbt run PASS=4, dbt test PASS=8; module 3 đạt 6/6. Unit suite: 9 tests, OK.
- Funnel: A 602 frame (đen 6, mờ 9, tối 4, gần trùng 149, mất đồng bộ 7); B 644 (4, 0, 61, 53, 7). Gap sync trung bình/tối đa: A 66/2.884 ms, B 73/3.112 ms.
- Ngưỡng mặc định: dark 12/40, blur 40, dHash 2 bit, sync 150 ms. Cờ chồng lấp nên không cộng trực tiếp để suy ra số bị loại.
- Idempotency trip-00003: 1.246→1.246, fingerprint `6c7480dfde7e` không đổi; append sai tạo 120 dòng cho partition 60 frame.
- Threshold A/B ở 0 bit = 61/8; 2 bit = 149/53; 6 bit = 363/244. Giữ cấu hình mặc định 2.
- Câu 4–5 và bảng đã điền từ `step3.txt`, `unit_tests.txt`, `idempotency.json`, `threshold.json` và code fingerprint/SQL flag.
- Bước tiếp theo: CP5 chạy Gold, export lặp, manifest và leakage.
