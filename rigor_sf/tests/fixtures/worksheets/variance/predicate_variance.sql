SELECT *
FROM app.events e
JOIN app.windows w
  ON e.event_ts >= w.start_ts
 AND e.event_ts < w.end_ts
JOIN app.users u
  ON LOWER(e.user_email) = LOWER(u.email)
JOIN app.orders o
  ON CAST(e.order_id AS VARCHAR) = o.id;
