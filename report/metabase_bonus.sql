SELECT drive_id, lighting, conditions,
       SUM(frames) AS frames, SUM(frames_kept) AS frames_kept,
       SUM(frames) - SUM(frames_kept) AS frames_dropped,
       ROUND(100.0 * (SUM(frames) - SUM(frames_kept)) / NULLIF(SUM(frames), 0), 2) AS dropped_pct
FROM marts.mart_trip_summary
GROUP BY drive_id, lighting, conditions
ORDER BY dropped_pct DESC, drive_id, lighting, conditions;
