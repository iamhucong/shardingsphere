SELECT SUM(user_id) AS user_id_sum FROM t_order;
SELECT SUM(t_order.user_id) AS user_id_sum FROM t_order;
SELECT COUNT(*) AS orders_count FROM t_order;
