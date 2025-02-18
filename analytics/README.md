## Analytical Queries & Visualizations

### 1. Top 10 played songs
```sql
SELECT s.title AS song_name, COUNT(sp.song_id) AS play_count
FROM songplays_fact sp
JOIN songs_dim s ON sp.song_id = s.song_id
GROUP BY s.song_id, s.title
ORDER BY play_count DESC
LIMIT 10;
```
<img src="results_charts/query-1r.png" alt="Query 1r" width=auto height="220">
<img src="results_charts/query-1.png" alt="Query 1" width="700" height=auto>

### 2. Top 10 song plays per user
```sql
SELECT user_id, COUNT(*) AS play_count
FROM songplays_fact
GROUP BY user_id
ORDER BY play_count DESC
LIMIT 10;
```
<img src="results_charts/query-2r.png" alt="Query 2r" width=auto height="220">

### 3. Retrieve user info of the top 10 song plays per user
```sql
SELECT u.user_id, u.first_name, u.last_name, COUNT(sp.songplay_id) AS play_count
FROM songplays_fact sp
JOIN users_dim u ON sp.user_id = u.user_id
GROUP BY u.user_id, u.first_name, u.last_name
ORDER BY play_count DESC
LIMIT 10;
```
<img src="results_charts/query-3r.png" alt="Query 3r" width=auto height="220">

### 4. Daily song plays within a date range
```sql
SELECT DATE(sf.start_time) AS play_date, COUNT(*) AS daily_plays
FROM songplays_fact sf
INNER JOIN songs_dim s ON sf.song_id = s.song_id
INNER JOIN artists_dim a ON s.artist_id = a.artist_id
WHERE start_time >= '2018-11-01 00:00:00' AND start_time <= '2018-11-10 23:59:59'
GROUP BY play_date
ORDER BY play_date;
```
<img src="results_charts/query-4r.png" alt="Query 4r" width=auto height="220">

<img src="results_charts/query-4.png" alt="Query 4" width="700" height=auto>

### 5. Unique users per subscription level
```sql
SELECT level, COUNT(DISTINCT user_id) AS unique_users
FROM users_dim
GROUP BY level;
```
<img src="results_charts/query-5r.png" alt="Query 5r" width=auto height="60">

<img src="results_charts/query-5.png" alt="Query 5" width="700" height=auto>

### 6. Top paid listeners
```sql
SELECT COUNT(sp.songplay_id) AS play_counts, u.user_id, u.first_name, u.last_name
FROM songplays_fact sp
JOIN users_dim u ON sp.user_id = u.user_id
WHERE u.level = 'paid'
GROUP BY u.user_id, u.first_name, u.last_name
ORDER BY play_counts DESC;
```
<img src="results_charts/query-6r.png" alt="Query 6r" width=auto height="460">

<img src="results_charts/query-6.png" alt="Query 6" width="700" height=auto>

### 7. Unique users who played at least one song
```sql
SELECT COUNT(DISTINCT user_id) AS unique_users
FROM songplays_fact;
```
<img src="results_charts/query-7r.png" alt="Query 7r" width=auto height="40">

