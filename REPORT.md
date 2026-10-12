# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Hồng Cường |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602415` |
| GitHub | `hongcuong26-debug` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-05297` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

- Lần reset này Bronze nhận 3.021 dòng A và 3.234 dòng B (`report/logs/step1.txt`, bảng “Số dòng”).
- A dùng `ts_utc`, `speed_mps`, `heading_deg`, `lat/lon`, `alt_m`, `acc_x/acc_y`, `gas/brake/steer`; B lần lượt dùng `timestamp_ms`, `spd_kmh`, `hdg`, `latitude/longitude`, `altitude`, `ax/ay`, `throttle_pct/brake_pct/steering`.
- Timestamp A là chuỗi ISO UTC, còn B là epoch millisecond; tốc độ A là m/s, B là km/h; bàn đạp A là tỷ lệ 0–1, B là phần trăm. `bronze_video_files` còn ghi offset đồng hồ B = 1.500 ms, A = 0 (`report/logs/bronze_video_files.txt`).
- Giữ hai bảng và mọi trường raw dạng chữ giúp bảo toàn đúng dữ liệu nguồn, không mất dòng vì ép kiểu/schema drift; việc chuẩn hóa có kiểm soát được thực hiện ở Silver.
- Lượt nạp lại drive A báo `already present`; history `bronze.raw_telemetry_a` chỉ có version 0, một WRITE thêm 3.021 dòng (`report/logs/bronze_history.txt`), nên lần lặp không tạo commit hay dòng mới.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

- Model A cast ISO UTC và giữ tốc độ m/s; model B đổi epoch ms sang `timestamptz`, chia `spd_kmh/3.6`, chia throttle/brake phần trăm cho 100 và đổi tên các cột về schema chung.
- B còn trừ `telemetry_clock_offset_ms=1500`: telemetry sạch bắt đầu `2025-10-08 14:30:00.009+00`, chỉ sau video start `14:30:00.000` 9 ms (`report/logs/telemetry_clock.txt`).
- Quarantine quan sát được: drive A = duplicate 20, speed_missing 30, speed_out_of_range 3; drive B cũng lần lượt 20, 30, 3 (`report/logs/step2.txt`).
- Vì vậy A có 2.968 sạch + 53 reject = 3.021 raw; B có 3.181 + 53 = 3.234. Module 2 kiểm lại các đẳng thức này và đạt 7/7.
- Giữ rejects cùng `drive_id`, timestamp, `_batch_id`, `_row_no` và lý do giúp audit/sửa nguồn hoặc đổi quy tắc; xóa im lặng sẽ mất lineage và che tỷ lệ lỗi.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

- `max_gap_ms` là `warn` vì khoảng mất tín hiệu đã biết cần được báo để theo dõi nhưng chưa nhất thiết làm vô hiệu toàn dataset; `in_range` là `error` vì tốc độ ngoài 0–70 m/s làm sai dữ liệu nghiệp vụ và phải chặn downstream.
- Khi nới bước làm sạch lên 1.000 m/s, `break_gate.json` ghi `gate_failed=true`; test `in_range_silver_telemetry_speed_mps__70__0` bắt đúng 6 dòng.
- Pha nới có `PASS=5 WARN=1 ERROR=1`; sau trả ngưỡng gốc có `PASS=6 WARN=1 ERROR=0`, nên lỗi cố ý không còn trong trạng thái nộp.
- Thí nghiệm cho thấy cổng quality độc lập với logic làm sạch: nếu phép biến đổi vô tình cho dữ liệu vô lý đi qua, test mức `error` vẫn dừng pipeline trước Gold.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

- Với `trip-00003`, bảng frames có 1.246 dòng trước và vẫn 1.246 dòng sau hai lần chạy lại; fingerprint giữ nguyên `6c7480dfde7e`, nên `unchanged=true`.
- Trip đó có 60 frame; biến thể ghi `append` hai lượt tạo 120 dòng, minh họa nhân đôi đúng 2× chứ không phải so với tổng 1.246 dòng toàn bảng.
- Delta `overwrite` có predicate `trip_id` thay đúng một partition trong một transaction: dữ liệu cũ còn nguyên cho tới commit mới thành công và retry cho cùng kết quả.
- Cách “xóa rồi ghi” tách hai thao tác có cửa sổ lỗi để partition bị trống/dở dang. Fingerprint chỉ băm trường nghiệp vụ (`frame_id`, trip, brightness, blur, dHash), không gồm `_processed_at`, nên chứng minh nội dung ổn định chứ không khẳng định metadata thời gian không đổi.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

- Ngưỡng brightness <12 là đen và bị loại; từ 12 đến <40 là tối nhưng vẫn giữ vì cảnh đêm/hầm là dữ liệu thật. Quan sát: A có 6 đen/4 tối, B có 4 đen/61 tối.
- Blur dùng variance <40 (nhưng không gắn blur cho frame đã đen); near-duplicate dùng khoảng cách dHash với frame trước trong cùng trip; sync hợp lệ khi gap ≤150 ms.
- `threshold.json` cho thấy tăng ngưỡng 0→2→6 làm số frame bị coi trùng tăng A: 61→149→363, B: 8→53→244.
- Tôi giữ ngưỡng mặc định 2: nó bắt chuỗi đứng hình/gần giống nhưng ít có nguy cơ bỏ chuyển động thật hơn ngưỡng 6. Xe chạy chậm tạo nhiều frame gần nhau nên cùng ngưỡng sẽ loại tỷ lệ cao hơn; camera đứng hình làm dHash giống hệt.
- Các cờ có thể chồng lấp nên không cộng 6+9+149+7 để suy ra số frame bị loại; `dark_kept` chỉ là số frame mang cờ tối, không bảo đảm mọi frame đó qua Gold nếu đồng thời có cờ khác.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

- `leakage.json` ghi 1.019 khung đáng dùng thuộc 26 trip. Nếu chia ngẫu nhiên theo từng khung, cả 26 trip đều xuất hiện ở nhiều split và cả 26 trip có khung ở cả train lẫn test; chia theo trip của Gold có `per_trip_split_trips_straddling=0`.
- Các frame liền nhau trong cùng chuyến có cảnh vật, góc nhìn và điều kiện gần như giống nhau. Khi chúng nằm ở cả train và test, mô hình có thể ghi nhớ các mẫu gần trùng với “đề thi”, nên chỉ số test đẹp nhưng không phản ánh khả năng tổng quát sang chuyến mới.
- Truy vấn độc lập `count(distinct split)` theo `trip_id` không trả dòng nào (`report/logs/gold_no_straddle.txt`); thí nghiệm chỉ chứng minh nguy cơ leakage, không đo hay khẳng định accuracy tăng bao nhiêu.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

- `pipeline/publish.py` sắp Gold theo `frame_id`, ghép mỗi dòng thành `frame_id|split`, nối bằng xuống dòng, lấy SHA256 rồi giữ 12 ký tự đầu. Vì vậy version đại diện đúng tập frame và phép gán split, không phụ thuộc đồng hồ hay thứ tự chạy; nó không phải hash byte toàn bộ ảnh/metadata.
- Export ở Bước 4 và export lại liên tiếp đều cho `b2fb840da0d5`, cùng 1.019 frame và train/val/test = 693/164/162 (`step4.txt`, `export_second.txt`). `created_at`, Delta version hoặc manifest có thể đổi khi chạy lại; kết luận ổn định ở đây chỉ dành cho `dataset_version` và các số đã so.
- SQL Gold xếp hạng trip riêng trong từng drive bằng `md5(trip_id || '2A202602415')`, rồi chia cả trip vào train/val/test. MSSV khác có thể đổi thứ tự trip, split và do đó đổi hash version; không có nghĩa mọi cặp MSSV chắc chắn tạo version khác nhau.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

- Manifest thật của `b2fb840da0d5` chỉ ra upstream: `bronze.raw_telemetry_a=0`, `bronze.raw_telemetry_b=0`, `bronze.trips_raw=0`, `bronze.video_files=1`, `silver.frames=27`, `silver.telemetry=0`, `silver.frames_flagged=0`, `silver.frame_telemetry=0` (`report/logs/manifest_cp5.txt`).
- Hai video đầu vào có SHA256: drive A `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`, drive B `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Manifest cũng ghi quy tắc loại black/blurry/near-duplicate/out-of-sync và chia theo trip, stratified theo drive, thứ tự MD5.
- Để tái tạo sau ba tháng cần giữ đúng commit/config pipeline, MSSV `2A202602415`, dữ liệu nguồn có các hash trên, các Delta table/version lineage, quy tắc lọc/chia và manifest/parquet. Bản quan sát có 1.021 object: 1.019 ảnh đúng 693/164/162 cùng `manifest.json` và `manifest.parquet`; CP9 sẽ tải lại manifest cuối để đối chiếu trạng thái nộp.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

- Do MSSV có tiền tố chữ, tôi giữ nguyên `2A202602415` cho toàn bộ pipeline nhưng dùng phần số `202602415 % 26 = 15` để chọn `trip-05297`; run lỗi là `cp7_crash_trip05297_20261013T001900`.
- Đúng một mapped task `silver_frames[15]` chuyển đỏ; 7 task downstream thành `upstream_failed`. Log thật ghi `simulated crash while extracting trip-05297` và ảnh `fail_task_log.png` cho thấy đúng task/trip này.
- Tôi clear riêng mapped instance lỗi trước. Instance chạy lại xanh, nhưng Airflow 2.10.5 không tự mở lại 7 task đã ở trạng thái `upstream_failed`, nên tôi clear chính xác 7 task downstream đó trong cùng logical date; không reset lake và không `mark success`.
- Run sau đó thành `success`; ảnh `airflow_retry_graph.png` cho thấy `silver_frames [26]` và toàn bộ downstream xanh. Hook được gọi trước vòng decode/upload, nên đây chỉ là mô phỏng lỗi lúc bắt đầu extraction, không chứng minh atomicity nếu sập giữa commit.
- Trước và sau retry đều có 1.019 frame, `dataset_version=b2fb840da0d5`; truy vấn Gold cho 0 `frame_id` trùng và verify đạt `27/27` (`report/logs/cp7_verify.txt`). Cuối cùng `LAB_FAIL_ONCE` đã được xóa và container báo `HOOK_EMPTY`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
