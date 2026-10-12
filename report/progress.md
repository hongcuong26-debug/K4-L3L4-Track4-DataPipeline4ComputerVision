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
