# Data Pipeline for Computer Vision — Codex Execution Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to execute this plan task-by-task. Steps use checkbox syntax for tracking. Chạy tuần tự trong cùng môi trường; đây là bài vận hành có trạng thái dùng chung, không chia các bước pipeline cho nhiều agent.

**Goal:** Hoàn thành bài cá nhân bằng pipeline có sẵn, bằng chứng thực nghiệm thật và REPORT.md khớp MSSV; đạt 27/27, DAG xanh và không còn FAIL ở checker.

**Architecture:** Giữ nguyên pipeline ingestion → Bronze → Silver → quality gate → Gold → Postgres → Metabase. Codex chạy CLI sẵn có, đọc code để giải thích, lưu đầu ra và điền báo cáo; Airflow vận hành cùng pipeline theo từng task.

**Tech Stack:** Docker Desktop/Compose v2, Airflow 2.10.5, MinIO, Delta Lake, FFmpeg, DuckDB/dbt, Postgres 16, Metabase, Python 3 và Git.

**Spec:** Đề bài người dùng đã cung cấp cùng README.md, GUIDE.md, REPORT.md và RUBRIC.md trong repo đích. Đọc lại bản local lúc thực thi; nếu khác bản đã khảo sát, ghi rõ khác biệt trước khi áp dụng lệnh.

**Repo đích:** https://github.com/hongcuong26-debug/K4-L3L4-Track4-DataPipeline4ComputerVision
**Baseline đã đọc ngày 12/10/2026:** main tại c8e49c19b2ebe82fe04b50e332bcc33bd0bccf4e.
**Phạm vi phiên hiện tại:** Chỉ lập kế hoạch. Chưa chạy Docker, chưa thực nghiệm, chưa sửa hoặc push repo.

## 1. Global Constraints

- Bài này KHÔNG yêu cầu viết pipeline mới. Không thêm model, training, API, UI hay framework.
- Không sửa tests/, pipeline/verify.py, docker-compose.yml hoặc scripts/protected.json; rubric trừ 20 điểm khi sửa các file bị bảo vệ.
- Mặc định không sửa pipeline/, dbt/, dags/, docker/, scripts/: giảng viên ghi đè các thư mục đó bằng bản gốc khi chấm. Không chữa bài bằng cách thay logic đề.
- Chỉ thay .env cục bộ, REPORT.md và tạo artifacts report/; chỉnh Git index để bỏ dữ liệu khỏi bài nộp.
- MSSV phải là cùng một chuỗi trong .env, container, REPORT.md và cả năm JSON. Không dùng 2024000 trong .env.example làm MSSV thật.
- Chưa có MSSV xác nhận trong yêu cầu này. Đọc .env và REPORT.md hiện hữu; nếu thiếu hoặc mâu thuẫn, hỏi đúng MSSV. Không suy đoán từ tên repo hay bài trước.
- Họ tên dự kiến Nguyễn Hồng Cường, GitHub hongcuong26-debug; đối chiếu thông tin hiện hữu trước khi ghi.
- Không bịa số liệu, screenshot, trạng thái DAG, cảm nhận cá nhân hay slide chưa đọc.
- Dùng dataset chuẩn qua MANIFEST.json. Không tự tạo bộ dữ liệu thay thế để báo đạt.
- Không commit .env, data video/CSV/sources.json, dataset ZIP, work artifacts, .tar, .duckdb, token hoặc khóa thật. Giữ data/MANIFEST.json và các .gitkeep có sẵn.
- Không xóa .git, git init lại repo đang có, force-push, sửa ngày commit hoặc tạo commit rỗng để đủ số lượng.
- Commit theo tiến trình thực, có ít nhất 5 commit bài làm, thông điệp nêu Bước 1–5. Commit từ repo đề không thay cho lịch sử làm bài của học viên.
- --reset được phép trong stack lab riêng và các checkpoint chỉ định; không reset dịch vụ/dữ liệu của dự án khác.
- Không chạy CLI, experiments, export và DAG đồng thời: chúng cùng ghi lake, DuckDB và warehouse.
- Lỗi môi trường thì chẩn đoán môi trường trước. Không sửa test/chốt verify thành true để vượt checkpoint.
- Chỉ thực thi push nếu phiên thực thi có quyền push repo đích. Plan này không tự cấp quyền thay đổi visibility, tạo repo khác hoặc gửi bài cho giảng viên.

## 2. Review Focus — kiểm tra bổ sung bắt buộc

| Nguy cơ | Kiểm tra sở hữu |
|---|---|
| ZIP vốn đã tracked dù .gitignore đã có | CP0/CP1: git ls-files; bỏ khỏi index và giữ file local |
| MSSV có chữ làm int(MSSV) lỗi | CP0/CP6: giữ nguyên MSSV cho split, xác minh cách tính trip với giảng viên nếu không phải số |
| --save-verify thoát 0 ngay cả khi verify thất bại | CP9: chạy verify trực tiếp, kiểm tra exit code và nội dung 27/27 |
| Screenshot/lineage/báo cáo lấy từ các trạng thái khác nhau | CP6/CP9: lưu run ID, version trước/sau, manifest cuối và đối chiếu |
| Checker quá sơ sài: 4 ảnh hoặc báo cáo dài vẫn có thể sai | CP8/CP9: kiểm tra thủ công 5 ảnh thật, 10 câu trả lời và số liệu |

## 3. File map

| File / thư mục | Hành động |
|---|---|
| README.md, GUIDE.md, RUBRIC.md, REPORT.md | Đọc nguyên văn; chỉ điền REPORT.md, giữ cấu trúc |
| .env | Tạo nếu thiếu; đặt LAB_STUDENT_ID; tạm thêm rồi xóa LAB_FAIL_ONCE |
| scripts/lab.sh, check_data.py, check_submission.py, protected.json | Đọc và chạy; không sửa |
| pipeline/steps.py, experiments.py, frames.py, publish.py, verify.py | Đọc để hiểu đầu ra, version, fail-once; không sửa |
| dbt/models/staging/stg_telemetry_a.sql, stg_telemetry_b.sql | Đọc schema drift, đổi đơn vị và clock offset |
| dbt/models/schema.yml, dbt/dbt_project.yml | Đọc quality gate và ngưỡng mặc định |
| dbt/models/gold/gold_dataset_frames.sql | Đọc split theo trip và MSSV |
| dbt/models/marts/mart_trip_summary.sql, mart_frame_funnel.sql | Đọc schema trước khi dùng SQL Metabase |
| dags/day09_video_pipeline.py | Đọc mapped task, dependency và reset DAG |
| report/evidence/ | 5 JSON do lệnh gốc sinh ra; không sửa bằng tay |
| report/screenshots/ | 5 ảnh UI thật: graph, lake, dashboard, log lỗi, graph phục hồi |
| report/logs/ | Log CLI theo bước, truy vấn, kiểm tra và run ID; tránh ghi token/khóa |
| report/manifest_final.json | Bản sao manifest cuối tải từ lake, dùng trả lời lineage |
| report/metabase_question.sql | SQL câu hỏi riêng đã thực sự chạy và thêm vào dashboard |
| report/progress.md | Nhật ký checkpoint, blockers, trạng thái cuối để tiếp tục phiên sau |
| report/verify.txt | Do --save-verify sinh; phải đọc để xác nhận 27/27 |
| docs/superpowers/plans/2026-10-12-cv-data-pipeline-plan.md | Chép plan này vào repo khi bắt đầu thực thi |

Không cần viết hoặc sửa test; chạy 9 unit test sẵn có. Bảng file tạo thêm không phải thay thế 5 JSON và 5 ảnh bắt buộc.

## 4. Quy ước thực thi

Lệnh bên dưới dùng Bash tại repo root. Windows: chọn WSL2 hoặc Git Bash có Docker CLI hoạt động; không dán nguyên shell Bash vào CMD/PowerShell. Host chỉ cần Python chuẩn cho check_data/check_submission/setup_metabase; lab.sh chạy thư viện pipeline trong venv của container Airflow.

Ở Windows kiểm tra khả năng chạy scripts/lab.sh bằng checker. Ưu tiên chạy checker trong WSL/Linux nếu môi trường host không hỗ trợ shebang. Không sửa scripts/check_submission.py để né lỗi môi trường.

Nếu thiếu unzip:
~~~bash
python3 -m zipfile -e dataset/day09-data.zip .
~~~
Zip đã chứa data/, không giải nén thành data/data/.

Lưu log Bash với pipefail để không che exit code:
~~~bash
set -o pipefail
mkdir -p report/logs report/evidence report/screenshots
scripts/lab.sh step 2 2>&1 | tee report/logs/step2.txt
~~~
Áp dụng mẫu này cho các lệnh quan trọng với tên log riêng. Sau mỗi checkpoint ghi lệnh, exit code, đầu ra, thông tin đã hiểu, file tạo ra và bước tiếp theo vào report/progress.md.

Mọi câu trả lời có số liệu phải có đường dẫn nguồn và key/query. Không lấy câu “CẦN THẤY” trong log hoặc số ví dụ trong GUIDE làm kết quả đo được.

## CP0 — Repo, danh tính, tài nguyên và trạng thái ban đầu

**Consumes:** Repo local hoặc URL đích. **Produces:** Baseline, môi trường đã xác định, danh tính xác nhận và blockers rõ ràng.
**Files:** Đọc tài liệu/AGENTS.md nếu tồn tại; tạo plan local và report/progress.md.

- [ ] Xác định repo root, đọc AGENTS.md áp dụng và các tài liệu lab. Tại baseline khảo sát không có AGENTS.md ở root.
- [ ] Nếu đã có repo local, dùng chính checkout đó; xem git status trước khi làm, giữ mọi thay đổi của người dùng. Nếu chưa có, clone repo đích vào thư mục home.
- [ ] Ghi git rev-parse HEAD, git branch --show-current, git remote -v, git log -5 --oneline, git status --short.
- [ ] Đảm bảo origin là repo đích; không đổi sang repo giảng viên. Repo đích hiện được GitHub báo Public khi khảo sát; kiểm tra lại lúc nộp.
- [ ] Đọc .env nếu có nhưng không in toàn bộ bí mật. Xác nhận LAB_STUDENT_ID và thông tin cá nhân. Thiếu MSSV thì hỏi một lần; trong lúc đợi vẫn hoàn thành kiểm tra repo, Docker và dataset.
- [ ] Kiểm tra Docker Desktop đang chạy, docker version, docker compose version, python3 --version và Git. Ghi dung lượng ổ trống.
- [ ] Kiểm tra Docker có ≥6 GB RAM và khoảng 8 GB ổ trống theo đề. Máy 8 GB tổng RAM có thể thiếu dư địa: bật Metabase sau cùng; báo đúng nếu OOM.
- [ ] Kiểm tra có stack day09 đang chạy từ thư mục khác không. Compose name của đề là day09 nên clone thứ hai có thể đụng stack; chỉ chạy một checkout.
- [ ] Đọc scripts/protected.json và xác minh SHA256 từng file được bảo vệ. Nếu đã bị sửa, báo đường dẫn và diff; không ghi lại protected.json.
- [ ] Chạy git ls-files dataset/day09-data.zip. Bản repo khảo sát đang tracked ZIP; lập xử lý CP1.
- [ ] Tạo tiến trình ban đầu và chép plan này vào docs/superpowers/plans/.

**Gate:** MSSV đã xác nhận trước mọi run sinh split/bằng chứng; stack đúng dự án; file bảo vệ nguyên vẹn. Nếu chưa có Docker, vẫn có thể hoàn thành kiểm tra tĩnh nhưng chưa thể tuyên bố đạt lab.

## CP1 — Chuẩn bị dữ liệu và smoke run toàn pipeline

**Consumes:** CP0, MSSV thật. **Produces:** Dữ liệu chuẩn, dịch vụ sẵn sàng, smoke run 27/27.
**Files:** .env local; report/logs/preflight.txt, smoke_all.txt; report/evidence/run_summary.json tạm.

- [ ] Nếu chưa có .env, sao chép .env.example. Đặt LAB_STUDENT_ID bằng chuỗi MSSV đã xác nhận; không ghi đè các key khác.
- [ ] Giải nén ZIP từ repo root; nếu thiếu ZIP chạy python3 scripts/fetch_data.py.
- [ ] Chạy python3 scripts/check_data.py; mọi file trong MANIFEST phải khớp SHA256. Sai hash thì lấy lại dataset chuẩn, không tự sửa MANIFEST.
- [ ] Giữ ZIP trên máy nhưng bỏ khỏi index nếu đang tracked:
~~~bash
git rm --cached -- dataset/day09-data.zip
git check-ignore dataset/day09-data.zip
~~~
Không xóa bản local, không rewrite lịch sử. .gitignore không tự bỏ file đã tracked.
- [ ] Rà git ls-files để tìm data/CSV/video/ZIP/work đã tracked và loại đúng artifacts cấm khỏi index, không bỏ data/MANIFEST.json hoặc .gitkeep.
- [ ] Chạy docker compose --profile orchestrate up -d --build; kiểm tra docker compose ps và health endpoints. Airflow /health HTTP 200 chưa đủ: xem scheduler/database status hoặc kiểm tra scheduler thực sự hoạt động.
- [ ] Đợi sẵn sàng theo lần ngắn; nếu kéo image chậm, OOM hoặc lỗi build, lưu log thật và chẩn đoán. Không thay image/version trong Compose để “làm cho pass”.
- [ ] Chỉ kiểm tra biến cần thiết trong container:
~~~bash
docker compose exec -T airflow printenv LAB_STUDENT_ID
~~~
Chuỗi phải khớp .env.
- [ ] Khi không có DAG đang chạy, chạy scripts/lab.sh step all --reset và lưu smoke_all.txt. Đây là chạy CLI, chưa chứng minh Airflow DAG xanh.
- [ ] Kiểm tra exit code 0, 27/27 trong đầu ra và run_summary.json.verify_all_passed = true.
- [ ] Commit thay đổi nhận đề/plan/bỏ ZIP khỏi index: chore: chuẩn bị lab day09 và dữ liệu local.

**Gate:** Dữ liệu đúng, smoke đạt, chưa dùng run_summary tạm làm kết quả cuối.

## CP2 — Bước 1: Bronze và schema drift

**Consumes:** CP1. **Produces:** Bằng chứng ingest lặp, schema thô và bản nháp Câu 1.
**Files:** report/logs/step1.txt, bronze_history.txt, bronze_video_files.txt; REPORT.md.

- [ ] Chạy scripts/lab.sh step 1 --reset, lưu đầu ra. Reset này xóa lake; chưa chạy DAG/Metabase song song.
- [ ] Tìm already present thật. steps.py hiện chỉ nạp lại drive_a ở lần kiểm tra lặp; không viết “đã nạp cả hai drive hai lần” nếu đầu ra không chứng minh.
- [ ] Chạy và lưu:
~~~bash
scripts/lab.sh cli history bronze.raw_telemetry_a
scripts/lab.sh cli sql "select drive_id, telemetry_clock_offset_ms, sha256 from lake.bronze_video_files"
~~~
Lịch sử raw_telemetry_a phải có đúng 1 commit trong lượt chạy vừa reset, thường là version 0.
- [ ] Đọc sample A/B từ đầu ra. Lập bảng ≥4 khác biệt thực: ts_utc/timestamp_ms, speed_mps/spd_kmh, heading_deg/hdg, gas/throttle_pct, brake/brake_pct, lat/latitude...
- [ ] Đối chiếu schema Bronze là string để giữ raw, xử lý drift và lỗi ở Silver.
- [ ] Mở MinIO để xác định bronze/video/<drive>/raw.mp4 và bronze/delta/. Chưa cần dùng ảnh này làm ảnh lake cuối.
- [ ] Điền Câu 1 bằng quan sát có nguồn; commit: Bước 1: Bronze và bằng chứng ingest idempotent.

**Gate:** Module 1 đạt; ingest lặp không tạo dữ liệu mới; bảng khác biệt đúng các cột đã thấy.

## CP3 — Bước 2: Silver telemetry, quarantine và break-gate

**Consumes:** Bronze CP2. **Produces:** Module 2 đạt, break_gate.json và Câu 2–3.
**Files:** report/logs/step2.txt, rejects.txt, break_gate.txt; report/evidence/break_gate.json; REPORT.md.

- [ ] Chạy scripts/lab.sh step 2 và lưu log.
- [ ] Đọc stg_telemetry_a.sql/stg_telemetry_b.sql: B đổi km/h÷3.6, epoch ms sang thời gian và trừ offset; pedal %÷100. Offset lấy từ bronze.video_files.
- [ ] Đếm sạch/rejects theo drive và lý do; xác nhận clean + rejects bằng đầu vào. Lưu cả truy vấn rejects mẫu:
~~~bash
scripts/lab.sh cli sql "select * from silver.silver_telemetry_rejects limit 5"
~~~
Không ép kết quả về các số ví dụ; dữ liệu thực là nguồn.
- [ ] Đối chiếu min(ts) B sau chỉnh với video_start_utc thực tế; ghi offset và thời gian đo được.
- [ ] Đọc schema.yml để phân biệt warn max_gap_ms với error in_range; lưu kết quả dbt thực.
- [ ] Chạy scripts/lab.sh exp break-gate; command tổng phải thành công vì thí nghiệm chứng minh lỗi rồi khôi phục.
- [ ] Đọc gate_failed, failing_tests[].test/rows, summary_relaxed và summary_restored. Xác nhận lỗi in_range đã bắt tốc độ vượt 70, test sau khôi phục không ERROR.
- [ ] Kiểm tra lại scripts/lab.sh verify --module 2. Không để trạng thái nới 1000 m/s còn lại sau lỗi thí nghiệm.
- [ ] Điền Câu 2–3; commit: Bước 2: Silver telemetry và thí nghiệm quality gate.

**Gate:** break_gate.json thuộc MSSV thật; gate_failed true; restoration đạt. FAIL cố ý trong phase relaxed không phải cho phép FAIL ở trạng thái nộp.

## CP4 — Bước 3: Frames, unit test, idempotency và threshold

**Consumes:** Silver telemetry sạch. **Produces:** Module 3 đạt, 9 unit test đạt, hai JSON, Câu 4–5.
**Files:** report/logs/step3.txt, unit_tests.txt, idempotency.txt, threshold.txt; evidence tương ứng; REPORT.md.

- [ ] Chạy scripts/lab.sh step 3, rồi scripts/lab.sh test. Lưu 9 test và OK thực.
- [ ] Ghi số trip, tổng frames, cờ black/blurry/dark/near_dup/sync_lost và avg/max sync_gap_ms theo drive.
- [ ] Đọc dbt_project.yml: dark_luma 12.0, dark_soft 40.0, blur_var 40.0, dup_hamming 2, sync_tol_ms 150. Đọc điều kiện SQL thật trước khi diễn giải.
- [ ] Xem frame thật trong MinIO silver/frames/<drive>/ và đối chiếu frame_id/cờ chất lượng.
- [ ] Chạy scripts/lab.sh exp idempotency; đọc rows_before, rows_after_two_reruns, fingerprint_before/after, unchanged, rows_of_trip, append_variant_rows_after_two_runs.
- [ ] Xác nhận unchanged true, count và fingerprint bằng nhau; append_variant là bảng thử của riêng một trip, không so count đó trực tiếp với tổng bảng frames.
- [ ] Ghi rõ fingerprint bỏ _processed_at: nội dung nghiệp vụ ổn định dù timestamp xử lý/Delta history có thể đổi.
- [ ] Chạy scripts/lab.sh exp threshold; đọc per_drive[].within_0_bits, within_2_bits, within_6_bits. Điền bảng Câu 5 từ JSON.
- [ ] Giải thích ngưỡng cao coi nhiều frame gần giống là trùng, dễ bỏ biến đổi thật; tốc độ xe và camera đứng hình ảnh hưởng độ lặp. Lựa chọn ngưỡng là nhận xét; giữ cấu hình mặc định 2 cho bài nộp.
- [ ] Không cộng các cờ để tính tổng bị loại: một frame có thể mang nhiều cờ. Cột dark_kept trong summary là count(is_dark), không chứng minh mọi frame dark đều được giữ nếu đồng thời mờ/trùng/mất đồng bộ.
- [ ] Điền Câu 4–5 và commit: Bước 3: Frames, unit test và hai thí nghiệm.

**Gate:** 9 test OK; module 3 đạt; cả hai JSON hợp lệ, đúng MSSV và được tạo bởi CLI.

## CP5 — Bước 4: Gold, version, manifest và leakage

**Consumes:** CP4. **Produces:** Gold hợp lệ, version ổn định, leakage.json và Câu 6–8.
**Files:** report/logs/step4.txt, export_second.txt, leakage.txt; manifest bản quan sát; REPORT.md.

- [ ] Chạy scripts/lab.sh step 4 và lưu dataset_version, frames, by_split, trips_by_split.
- [ ] Chạy truy vấn kiểm tra trip chỉ ở một split:
~~~bash
scripts/lab.sh cli sql "select trip_id, count(distinct split) as n_splits from gold.gold_dataset_frames group by trip_id having count(distinct split) > 1"
~~~
Kết quả phải rỗng.
- [ ] Export lần nữa bằng scripts/lab.sh cli export-dataset; version phải bằng lần đầu. Lưu cả hai kết quả để chứng minh.
- [ ] Đọc gold/datasets/<version>/manifest.json trong MinIO; lưu bản sao, không dựng lại nội dung. Kiểm tra folder train/val/test và manifest.parquet.
- [ ] Chạy scripts/lab.sh exp leakage; đọc kept_frames, trips, per_frame_split_trips_straddling, per_frame_split_trips_in_both_train_and_test và per_trip_split_trips_straddling.
- [ ] Xác nhận per_trip_split_trips_straddling = 0. Không nói accuracy tăng bao nhiêu: lab này không train/evaluate model.
- [ ] Giải thích version chính xác theo publish.py: SHA256 của danh sách frame_id|split sắp thứ tự, lấy 12 ký tự. Không phải hash toàn bộ byte ảnh, toàn bộ metadata hay thời gian.
- [ ] created_at, Delta table version, lineage hoặc manifest_sha256 có thể đổi qua rerun trong khi dataset_version giữ nguyên; chỉ khẳng định ổn định đúng đại lượng đã so.
- [ ] Giải thích MSSV salt md5(trip_id||MSSV) để xếp trip theo drive; khác MSSV có thể đổi split/version, không chứng minh tuyệt đối mọi MSSV khác nhau đều có version khác nhau.
- [ ] Câu 8 ghi bảng upstream Delta versions và input_video_sha256 từ manifest quan sát, kèm commit/config/dataset/MSSV để tái tạo. Chốt lại bằng manifest cuối ở CP9.
- [ ] Điền Câu 6–8; commit: Bước 4: Gold, lineage và thí nghiệm leakage.

**Gate:** Module 4 đạt; hai export cùng version; leakage theo trip bằng 0. Không đổi MSSV cho bài bắt buộc.

## CP6 — Bước 5: Warehouse và chạy Airflow bình thường

**Consumes:** CP5; không có CLI run đang chạy. **Produces:** Warehouse, DAG từ lake trống xanh, baseline version trước crash.
**Files:** report/logs/step5.txt, airflow_baseline.txt; report/screenshots/airflow_graph.png.

- [ ] Chạy scripts/lab.sh step 5, xác nhận 6 marts và module 5 đạt.
- [ ] Mở http://localhost:8080, dùng tài khoản lab trong GUIDE. Bật day09_reset và day09_video_pipeline.
- [ ] LAB_FAIL_ONCE phải trống. Trigger reset DAG, đợi success; sau đó trigger pipeline DAG, đợi toàn run success.
- [ ] Xem silver_frames map thành 26 instance. Đây là DAG sử dụng max_active_tis_per_dagrun=1; không gọi 26 instance là 26 worker chạy song song.
- [ ] Ghi DAG run ID, thời điểm, task states và dataset_version từ lake; lưu graph xanh thật tại airflow_graph.png.
- [ ] Chạy scripts/lab.sh verify khi DAG đã xong; lưu 27/27. Lưu version và tổng frames trước crash.
- [ ] Tính trip riêng từ list-trips. Chỉ dùng công thức đề nếu MSSV toàn chữ số:
~~~bash
scripts/lab.sh cli list-trips | python3 -c "import sys,json; t=json.load(sys.stdin); print(t[int(sys.argv[1]) % 26])" "$LAB_MSSV_CONFIRMED"
~~~
LAB_MSSV_CONFIRMED là biến tạm do executor gán từ MSSV đã xác nhận; không mặc định .env đã export biến ra shell.
- [ ] Nếu MSSV có chữ: split vẫn nhận chuỗi đó, nhưng công thức int() của đề không nhận được. Không bỏ tiền tố, không đổi MSSV trong .env và không tự chọn hash thay công thức. Hoàn thành các phần độc lập, ghi blocker và xin quy tắc trip chính thức.
- [ ] Ghi trip xác định vào đầu REPORT.md và checkpoint.

**Gate:** DAG baseline thực sự success từ reset; có 26 mapped instance và baseline version. Verify CLI không thay thế bằng chứng DAG.

## CP7 — Airflow fail-once, clear đúng instance và phục hồi

**Consumes:** Baseline xanh và trip riêng hợp lệ. **Produces:** Log lỗi, graph phục hồi, số task states, version/count không đổi.
**Files:** report/screenshots/fail_task_log.png, airflow_retry_graph.png; report/logs/airflow_retry.txt; REPORT.md Câu 9.

- [ ] Thêm duy nhất LAB_FAIL_ONCE=<trip> trong .env rồi docker compose --profile orchestrate up -d; kiểm tra printenv LAB_FAIL_ONCE trong airflow.
- [ ] Trigger day09_reset; đợi success để xóa lake, DuckDB và marker .failed-*. Trigger pipeline mới.
- [ ] Đợi run ổn định ở trạng thái lỗi; ghi run ID, map index, task_id, trip, số failed và upstream_failed thực tế. Không lấy số upstream_failed từ sơ đồ dự đoán.
- [ ] Mở log của instance failed và tìm simulated crash while extracting <trip>; chụp ảnh đúng trip, log và trạng thái. Không tạo ảnh “minh họa”.
- [ ] Lưu fail_task_log.png trước khi clear. Không xóa fail-once marker hoặc reset lại stack trong lúc retry.
- [ ] Clear đúng mapped instance đỏ trong run đó, không clear 26 trip/whole DAG, không mark success thủ công. Ghi lựa chọn thực tế trong hộp clear.
- [ ] Theo dõi task extraction lỗi được chạy lại và downstream tiếp tục; đợi DAG success rồi chụp airflow_retry_graph.png.
- [ ] Nếu chỉ clear instance không khôi phục downstream ở phiên Airflow thực tế, ghi task states và log. Chẩn đoán scheduler/run; không reset toàn lake để giả “retry thành công”. Nếu phải clear downstream upstream_failed thì ghi rõ scope thực, không tuyên bố chỉ clear 1 instance; đánh dấu điểm khác hướng dẫn cần đối chiếu.
- [ ] Chạy scripts/lab.sh verify, so version/count với baseline CP6. Kiểm tra duplicate frame_id = 0 bằng truy vấn Gold hoặc Silver tương ứng.
- [ ] Đọc frames.py: hook lỗi hiện được gọi trước vòng decode/upload. Không viết thí nghiệm đã kiểm tra lỗi giữa lúc upload/Delta commit; chỉ mô phỏng lỗi extraction của một trip.
- [ ] Xóa LAB_FAIL_ONCE khỏi .env, áp dụng lại Compose và xác nhận biến container trống.
- [ ] Điền Câu 9 bằng run ID, số task failed/upstream_failed, scope clear thực, version trước/sau và ảnh liên quan.
- [ ] Commit: Bước 5: Warehouse, Airflow và phục hồi task lỗi.

**Gate:** 1 mapped extraction failed đúng trip; có ảnh lỗi và graph phục hồi thật; version/count ổn định; 27/27; fail switch đã xóa. Nếu thiếu UI/screenshot, ghi blocker, không báo hoàn tất.

## CP8 — Metabase, MinIO và câu hỏi SQL riêng

**Consumes:** CP7, warehouse cuối hợp lệ. **Produces:** Dashboard có câu hỏi riêng; ảnh lake và dashboard.
**Files:** report/metabase_question.sql; screenshots/metabase_dashboard.png, minio_lake.png; REPORT.md.

- [ ] Bật BI sau cùng:
~~~bash
docker compose --profile bi up -d
python3 scripts/setup_metabase.py
~~~
Đợi ready; mở dashboard URL mà script in ra. Nếu dashboard đã tồn tại, script không in lại login; xem tài khoản lab có sẵn trong setup_metabase.py.
- [ ] Kiểm tra schema marts thật và chạy câu hỏi vận hành: “Trip nào có tỷ lệ frame bị loại cao nhất, với số frame gốc/giữ lại và tốc độ trung bình?”.
~~~sql
SELECT drive_id, trip_id, lighting, conditions, frames, frames_kept,
       frames - frames_kept AS frames_dropped,
       ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 2) AS dropped_pct,
       avg_speed_kmh, split
FROM marts.mart_trip_summary
ORDER BY dropped_pct DESC NULLS LAST, trip_id
LIMIT 10;
~~~
SQL này dùng các cột đã đọc trong repo. Chạy trên Postgres/Metabase, không tự gọi nó là kết quả DuckDB nếu chưa thực sự query warehouse.
- [ ] Lưu SQL thành report/metabase_question.sql; lưu card trong Metabase và THÊM vào dashboard.
- [ ] Ghi các dòng kết quả nổi bật và diễn giải từ dữ liệu thật. Bảng này cho mức loại, chưa chứng minh nguyên nhân theo từng trip; nếu cần nói nguyên nhân phải truy vấn cờ frame tương ứng.
- [ ] Chụp dashboard có card riêng đọc được tại metabase_dashboard.png.
- [ ] Mở MinIO bucket lake, chụp Bronze/Silver/Gold tại minio_lake.png.
- [ ] Điền câu hỏi riêng. Với Câu 10, phân tích FFmpeg decode/I/O, DuckDB một node và Airflow/warehouse theo code và workload 500 xe; đề xuất workers phân tán, ELT phân tán/warehouse phù hợp, điều phối/serving tách tài nguyên. Không bịa benchmark scaling hoặc nội dung slide 12/22/23 nếu chưa có slide.
- [ ] Mục Nhìn lại: dùng những quan sát thật, không bịa cảm xúc của học viên. Để người dùng đọc/chỉnh nhận xét cá nhân trước nộp.
- [ ] Commit thay đổi dashboard/SQL/báo cáo: Bước 5: Dashboard và câu hỏi vận hành riêng.

**Bonus:** Chỉ sau phần bắt buộc đạt. Ưu tiên câu hỏi Metabase thứ hai có ý nghĩa, ví dụ tổng tỷ lệ loại theo drive/điều kiện từ mart_trip_summary. Không sửa dbt mặc định cho bonus trong plan này; tránh làm lệch version bài chính.

## CP9 — Run cuối, manifest cuối và đối chiếu báo cáo

**Consumes:** 4 JSON, 5 ảnh và báo cáo gần hoàn chỉnh. **Produces:** 5 JSON nhất quán, manifest cuối, verify.txt, không FAIL.
**Files:** report/evidence/run_summary.json cuối; report/manifest_final.json; report/verify.txt; REPORT.md.

- [ ] Xác nhận MSSV thật, fail switch trống, config mặc định và không có DAG đang chạy.
- [ ] Chạy scripts/lab.sh step all --reset, lưu report/logs/final_all.txt. Run này ghi đè run_summary.json; không sinh bốn JSON thí nghiệm.
- [ ] Nếu artifacts thí nghiệm cũ không cùng MSSV/input/config, chạy lại thí nghiệm cần thiết theo đúng dependency và khôi phục trạng thái chuẩn, rồi tạo run cuối lại. Không sửa student_id bằng tay.
- [ ] Tải manifest.json của version cuối từ lake thành report/manifest_final.json. Thay bảng lineage trong Câu 8 bằng bản cuối nếu Delta versions đã đổi.
- [ ] Đối chiếu version cuối với CP5/CP6/CP7; cùng input/MSSV/config phải ổn định. Nếu đổi thì điều tra trước khi chốt báo cáo.
- [ ] Đọc cả năm JSON và kiểm tra:
  - student_id = MSSV trong REPORT.md;
  - summary.verify_all_passed true;
  - REPORT version = summary.dataset_version = manifest.dataset_version;
  - REPORT train/val/test = summary.by_split và sum(by_split) = summary.frames;
  - idempotency unchanged true, counts/fingerprints bằng nhau, append so với rows_of_trip;
  - break_gate gate_failed true, failing_tests khớp Câu 3;
  - leakage per_trip = 0 và per_frame khớp Câu 6;
  - threshold các ngưỡng 0/2/6 khớp bảng Câu 5.
- [ ] Không khẳng định kiểm tra bốn JSON do --reset sinh mới; chúng lưu trên report mount và không bị xóa cùng lake.
- [ ] Chạy trực tiếp scripts/lab.sh verify và yêu cầu exit 0 cùng 27/27.
- [ ] Chạy:
~~~bash
python3 scripts/check_submission.py --save-verify
python3 scripts/check_submission.py
~~~
Đọc report/verify.txt; --save-verify hiện trả 0 ngay cả khi subprocess verify lỗi. File phải có 27/27 checks passed và không chứa lỗi trái với kết quả thành công.
- [ ] Kiểm tra file bảo vệ bằng SHA256; không dùng thay đổi protected.json để xác minh.
- [ ] Rà báo cáo: giữ nhãn MSSV, Trip dùng..., dòng dataset_version của bạn và ### Câu 1–10 để parser đọc được; mỗi câu 3–8 dòng giải thích thật, không còn placeholder/bảng trống.
- [ ] Kiểm tra 5 ảnh PNG mở được và nội dung đúng. Checker chỉ kiểm 3 stem và ≥4 ảnh; rubric cần cả log lỗi đúng trip VÀ graph xanh sau clear.
- [ ] Refresh dashboard sau run cuối; nếu kết quả/card hiển thị không khớp bản cuối thì chụp lại dashboard/MinIO. Ảnh lỗi/khôi phục giữ run lịch sử kèm run ID, không giả là ảnh run CLI cuối.
- [ ] Lưu tiến trình cuối, commit artifacts được phép: chore: hoàn thiện báo cáo và bằng chứng day09.
- [ ] Chạy checker một lần sau commit để xác nhận trạng thái sạch; log checker sau commit có thể giữ ngoài tracked tree để không tạo vòng “lưu log rồi dirty”.
- [ ] Nếu script báo PASS nhưng rubric còn thiếu ảnh/nội dung/DAG, sửa phần thiếu. Mốc chất lượng là cả rubric, không chỉ exit code checker.

**Gate:** Đủ bằng chứng thực; toàn bộ số khớp; ≥5 commit bài làm thật; checker không FAIL. Chưa push/nộp khi gate chưa đạt.

## CP10 — Bàn giao và kiểm tra repo remote

**Consumes:** CP9 đạt và quyền remote hợp lệ. **Produces:** Bài làm có thể review/nộp, trạng thái xác minh chính xác.

- [ ] Xem git diff --cached, git status --short, git ls-files và git log --oneline; không có ZIP/data/secrets/work artifacts trong cây commit cuối.
- [ ] Đếm commit bài làm sau baseline, không chỉ tổng số commit kế thừa. Có ≥5 commit thực với thông điệp các bước.
- [ ] Nếu phiên thực thi được giao hoàn thiện bài trên repo và có quyền push, push nhánh bài làm tới remote đích bằng quy trình Git bình thường; không force.
- [ ] Nếu auth/quyền push thiếu, báo local commit SHA và lệnh push cần chạy; không tuyên bố remote đã cập nhật.
- [ ] Kiểm tra repo Public và HEAD remote bằng công cụ GitHub/Git; kiểm tra khả năng truy cập clone không đăng nhập khi có thể.
- [ ] Nếu repo hiện Private, báo đó là blocker nộp bài; chỉ đổi visibility khi người dùng đã yêu cầu công khai repo này.
- [ ] Bàn giao link repo, commit SHA, verify 27/27, 9 tests, DAG run ID, 5 JSON, 5 ảnh, version/split và kết quả checker. Không gửi link cho giảng viên thay người dùng.
- [ ] Đưa danh sách phần chưa hoàn thành nếu có. Chỉ nói “sẵn sàng nộp” khi cả remote và rubric đạt.

## 5. Bảng nguồn cho báo cáo

| Nội dung | Bằng chứng cần dùng |
|---|---|
| Câu 1 | Step 1 sample A/B, history Bronze và video_files |
| Câu 2 | Step 2 clean/quarantine, staging SQL, clock offset và start timestamp |
| Câu 3 | schema.yml, break_gate.json: failing_tests, summary_restored |
| Câu 4 | idempotency.json; code overwrite/fingerprint; scope append là một trip |
| Câu 5 | Frames/cờ thực, threshold.json within_0/2/6_bits; không cộng cờ chồng lấp |
| Câu 6 | leakage.json: straddling và train/test overlap |
| Câu 7 | Hai export cùng version, publish.py hash frame_id|split và MSSV split salt |
| Câu 8 | manifest_final.json: lineage Delta versions, input_video_sha256 |
| Câu 9 | Run ID, task states/log, scope clear thực, ảnh lỗi/retry, version/count |
| Câu 10 | Kiến trúc/code thật; slide chỉ khi đã được cung cấp |
| Câu hỏi riêng | SQL đã chạy trên warehouse, card đã thêm dashboard và kết quả |
| Nhìn lại | Quan sát cụ thể, giới hạn chưa kiểm tra và nhận xét học viên xác nhận |

## 6. Khi bị chặn

| Tình huống | Hành động |
|---|---|
| Không có Docker daemon/Compose | Hoàn thành đọc repo/kiểm tra tĩnh; báo prerequisite và checkpoint dừng |
| Dataset thiếu/hash sai | fetch_data.py hoặc lấy ZIP chuẩn; không sửa hash/sinh số giả |
| Image pull/build lỗi | Lưu command/error, Docker logs; dùng image tar giảng viên nếu có |
| Airflow chưa ready/OOM | Kiểm tra scheduler, database, RAM và logs; bật BI sau cùng |
| Cổng bị chiếm | Xác định tiến trình/stack trùng; không sửa docker-compose.yml |
| dbt gate lỗi sau thí nghiệm | Kiểm tra restoration và chạy module 2 chuẩn; không hạ severity |
| MSSV có chữ | Giữ đúng chuỗi trong split; xin quy tắc tính fail trip tương thích |
| Không có browser điều khiển localhost | Tự động hóa phần CLI tiếp tục; yêu cầu người dùng thao tác UI cần thiết, không tạo ảnh giả |
| Chỉ có Codex cloud không truy cập Docker trên laptop | Không nhầm localhost cloud với laptop; bàn giao lệnh cho môi trường local |
| Retry không tự khôi phục downstream | Ghi thực trạng và thao tác thật; không che bằng full reset |
| Version thay đổi | So input hash, MSSV container, config, frame counts; không sửa version trong evidence |
| Checker FAIL | Sửa đúng artifacts/report/Git/env gây lỗi; không sửa checker |
| Quyền push bị chặn | Giữ local commits và báo blocker; không khẳng định repo remote hoàn tất |

## 7. Prompt để giao plan cho Codex

Sao chép file này vào docs/superpowers/plans/2026-10-12-cv-data-pipeline-plan.md của repo đang mở, rồi gửi:

~~~text
Dùng @Superpowers và skill executing-plans để thực hiện plan tại
docs/superpowers/plans/2026-10-12-cv-data-pipeline-plan.md.

Repo đích:
https://github.com/hongcuong26-debug/K4-L3L4-Track4-DataPipeline4ComputerVision

Đây là lab vận hành pipeline đã có, không yêu cầu viết mã mới.
Đọc AGENTS.md nếu có, README.md, GUIDE.md, REPORT.md, RUBRIC.md và plan đầy đủ.
Bắt đầu CP0, xác định môi trường Windows/WSL/Git Bash, Docker, Git và dữ liệu.
Đọc MSSV hiện có; nếu chưa xác nhận, hỏi tôi MSSV trước khi sinh bằng chứng.
Không sửa pipeline, tests, verifier, Docker Compose hoặc checker.
Giữ thay đổi sẵn có của tôi và không xóa .git/force-push.
Chạy tuần tự, dùng số liệu và screenshot thật; lưu tiến trình sau mỗi checkpoint.
Commit theo các bước thật, bỏ ZIP khỏi Git index nhưng giữ dataset local.
Làm đến CP10 trong phạm vi quyền hiện có; khi bị chặn vẫn hoàn thành phần độc lập,
báo lỗi cụ thể, không bịa PASS/bằng chứng và không hỏi lại những quyết định đã rõ.
Chỉ kết luận sẵn sàng nộp khi 27/27, 9 unit test, DAG xanh, 5 JSON, 5 ảnh,
REPORT khớp dữ liệu và checker không FAIL.
~~~

## 8. Self-review của plan

- Đã bao phủ chuẩn bị, 5 bước pipeline, 4 experiments, Airflow baseline/crash/retry, dashboard, 10 câu báo cáo, lineage cuối và Git/public submission.
- Lệnh/path/key SQL được đối chiếu với code repo tại baseline; chưa chạy nên mọi số liệu runtime được để executor thu thực tế.
- Không yêu cầu sửa pipeline/test để đạt điểm; không thêm chức năng không thuộc đề.
- Đã xử lý ZIP tracked, MSSV không phải số, --save-verify false success, cờ chất lượng chồng lấp, fingerprint/time metadata, version hash giới hạn và clear downstream.
- CP8/CP9 kiểm rubric thủ công vì checker không chứng minh nội dung screenshot, DAG success, hiểu biết báo cáo hoặc tính Public.


