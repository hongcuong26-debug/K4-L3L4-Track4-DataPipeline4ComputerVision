# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Hồng Cường |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602415` |
| GitHub | `hongcuong26-debug` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

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

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
