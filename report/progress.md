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

## CP5 — Bước 4: Gold, version, manifest và leakage

- `scripts/lab.sh step 4`: dbt run PASS=2, dbt test PASS=7; module 4 đạt 7/7. Gold có 1.019 frame, train/val/test = 693/164/162 và 26 trip = 18/4/4.
- Truy vấn chống straddle không trả dòng nào. Hai lần export liên tiếp đều cho `dataset_version=b2fb840da0d5`; lần thứ hai ghi `manifest_sha256=83d269ef1c553ec062a1608fc9ef621518fc95e7cdee99578c66bbc73a2b208a`.
- `leakage.json`: chia theo frame làm 26/26 trip bị cắt và cả 26 xuất hiện ở train lẫn test; chia theo trip làm 0 trip bị cắt.
- Manifest được đọc trực tiếp từ MinIO. Paginator xác nhận 1.021 object gồm 1.019 ảnh (693/164/162), `manifest.json` và `manifest.parquet`; lineage/version/hash video đã được đưa vào Câu 8.
- Một phép liệt kê S3 đầu tiên chạm giới hạn 1.000 object nên chỉ thấy 143 ảnh val; đã phát hiện tổng đúng 1.000 và chạy lại bằng paginator. `manifest_cp5.txt` hiện chỉ giữ kết quả đầy đủ.
- Hai lần thử interpreter sai (`python`, `/opt/venv/bin/python`) thất bại trước khi dùng đúng `/home/airflow/lab-venv/bin/python`; không thay đổi dữ liệu hay mã nguồn.
- Bằng chứng: `step4.txt`, `gold_no_straddle.txt`, `export_second.txt`, `leakage.txt`, `manifest_cp5.txt`, `report/evidence/leakage.json`.
- Bước tiếp theo: chạy gate module 4, commit CP5 rồi sang CP6 warehouse/Airflow baseline; quy tắc chọn fail trip vẫn cần xử lý vì MSSV có chữ.

## CP6 — Bước 5: Warehouse và Airflow baseline (phần độc lập đã xong)

- `scripts/lab.sh step 5` publish đủ 6 mart: trip summary 26, telemetry quality 2, split balance 3, speed coverage 7, gallery 1.019, funnel 2; module 5 đạt 2/2.
- `LAB_FAIL_ONCE` trống. Hai DAG đã được unpause; Airflow UI xác nhận `day09_video_pipeline` active và `Is paused=false`.
- Reset run `cp6_reset_2A202602415_20261012T235100` success từ 16:51:31 đến 16:52:42 UTC; task `reset_lake` success.
- Baseline run `cp6_baseline_2A202602415_20261012T235300` success từ 16:53:13 đến 17:01:09 UTC (7 phút 55 giây). Tất cả task success; `silver_frames` có đúng 26 mapped instance, chạy tuần tự do `max_active_tis_per_dagrun=1`.
- Sau DAG: `dataset_version=b2fb840da0d5`, 1.019 Gold frame, verify trực tiếp đạt 27/27. Ảnh graph xanh thật `airflow_graph.png` có kích thước 1.300×815, SHA256 `c49f1806d1305d27b0c3ac7141ea4f9505fd720deb1e9d1b40a253afb1dc67d2`.
- Airflow UI có lỗi JavaScript `Cannot set properties of undefined (setting 'autocomplete')`, làm toggle/link click không gửi thao tác. Đã dùng Airflow CLI chính thức để unpause/trigger, sau đó dùng UI đọc đúng run và chụp graph; không chạy pipeline CLI đồng thời với DAG.
- Lần polling đầu dùng `airflow dags state` với run ID không đúng cú pháp nên trả help text; đã dừng và dùng truy vấn read-only metadata Postgres. Log giữ cả lần thất bại và metadata đúng để audit.
- Bằng chứng: `step5.txt`, `airflow_baseline.txt`, `cp6_verify.txt`, `list_trips.txt`, `report/screenshots/airflow_graph.png`.
- Quy tắc trip được xử lý khi người dùng yêu cầu tiếp tục: giữ MSSV đầy đủ cho pipeline, dùng riêng phần số `202602415 % 26 = 15` để chọn trip-05297. Đây là giả định được ghi rõ trong REPORT.md, chưa có xác nhận quy tắc từ giảng viên.

## CP7 — Fail-once và phục hồi

- Run `cp7_crash_trip05297_20261013T001900`: một instance silver_frames[15] failed, bảy downstream upstream_failed; log lỗi đúng trip-05297.
- Clear riêng instance failed, rồi clear bảy downstream của đúng logical date vì scheduler giữ upstream_failed; cùng run phục hồi success, không reset/mark-success.
- Gold vẫn 1.019 frame, version b2fb840da0d5; duplicate frame_id=0; verify 27/27. Có ảnh log đỏ và Graph xanh sau retry. Fail hook đã xóa.

## CP8 — Metabase, MinIO và Bonus

- Dashboard `http://localhost:3000/dashboard/2` có sáu card mặc định và hai card vận hành mới ID 46/47; cả hai hiện dữ liệu warehouse cuối.
- Trip đứng đầu tỷ lệ loại: trip-04622, 34/60=56,67%. Bonus theo drive: A loại 166/602=27,57%, B 61/644=9,47%, chênh 18,10 điểm phần trăm.
- Cả hai drive chỉ thuộc nhóm Day/Clouds; không suy ra ảnh hưởng riêng của lighting/conditions. Bonus không thay đổi cấu hình hay version dataset.
- Đã mở và kiểm tra ảnh thật minio_lake.png (Bronze/Gold/Silver) và metabase_dashboard.png (hai card riêng đọc được). Câu 10, câu SQL riêng và Nhìn lại đã điền.

## CP9 — Run và kiểm tra cuối

- Lượt `scripts/lab.sh step all --reset` đầu bị ngắt do Airflow webserver startup timeout 120 giây (container exit=1, OOM=false); không sửa nguồn để né lỗi.
- Chạy lại cùng `python -m pipeline.cli step all --reset` trong one-off container cùng service/image/config/volumes, bỏ entrypoint webserver. run_summary cuối finished_at=2026-10-12T19:39:49.874073+00:00, verify_all_passed=true.
- Verify độc lập bằng one-off container đạt 27/27, exit 0; output thật lưu report/verify.txt. Unit suite chạy lại: 9 tests, OK.
- Version b2fb840da0d5, tổng 1.019, split 693/164/162, trip split 18/4/4; manifest cuối tải trực tiếp từ MinIO, lineage silver.frames=25. Bốn JSON thí nghiệm giữ lượt thực nghiệm riêng cùng MSSV; reset không tạo lại chúng.
- Bằng chứng cuối và báo cáo đã đối chiếu; mã nguồn pipeline/dbt/dags/docker/scripts/tests/docker-compose.yml không có diff.
- Chế độ --save-verify của checker gốc chạy qua adapter chỉ dành cho Windows: thay lời gọi scripts/lab.sh verify bằng cùng pipeline.cli verify trong one-off container; không sửa scripts/check_submission.py. Adapter yêu cầu exit 0 trước khi lưu file, khắc phục việc chế độ save của script không tự kiểm tra return code.
- Checker cuối đạt 17/17, không còn FAIL; WARN dirty được giải quyết bằng commit cuối. Đủ năm JSON, năm ảnh thật, REPORT.md 10 câu, câu SQL riêng, Bonus và Nhìn lại.
- Log terminal final_all.txt dừng ở output dbt marts do phiên shell bị đóng sau đó; run_summary.json hoàn tất thực tế lúc 19:39:49 UTC và verify độc lập 27/27 xác nhận trạng thái cuối. Không thêm output tưởng tượng vào log; các log PowerShell UTF-16 được chuyển cơ học sang UTF-8 cho GitHub đọc được.
