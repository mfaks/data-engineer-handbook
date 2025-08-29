-- Find duplicate records in game_details table
SELECT
	game_id, team_id, player_id, COUNT(1)
FROM game_details
GROUP BY game_id, team_id, player_id
HAVING COUNT(1) > 1;

-- Use a CTE to create a deduplicated version of game_details
-- The ROW_NUMBER() window function assigns a unique sequential integer to rows within a partition of a result set
-- Here, we partition by game_id, team_id, and player_id to identify duplicates
-- The WHERE clause filters to keep only the first occurrence of each duplicate set
WITH deduped AS (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id) AS row_num
	FROM game_details
)
SELECT * FROM deduped
WHERE row_num = 1

-- Final query to select deduplicated records with additional game information
WITH deduped AS (
	SELECT
		g.game_date_est,
		g.season,
		g.home_team_id,
		g.visitor_team_id,
		gd.*,
		ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.team_id, gd.player_id ORDER BY g.game_date_est) AS row_num
	FROM game_details gd
		JOIN games g on gd.game_id = g.game_id
)
SELECT 
	game_date_est,
	season,
	team_id,
	team_id = home_team_id AS dim_is_playing_at_home,
	player_id,
	player_name,
	start_position,
	comment,
	min,
	fgm, 
	fga,
	fg3m,
	fg3a,
	ftm,
	fta,
	oreb,
	dreb,
	reb,
	ast,
	stl,
	blk,
	"TO" AS turnovers,
	pf,
	pts,
	plus_minus
FROM deduped
WHERE row_num = 1

-- Create the fact table for game details
CREATE TABLE fct_game_details (
	dim_game_date DATE,
	dim_season INTEGER,
	dim_team_id INTEGER,
	dim_player_id INTEGER,
	dim_player_name TEXT,
	dim_start_position TEXT,
	dim_is_playing_at_home BOOLEAN,
	dim_did_not_play BOOLEAN,
	dim_did_not_dress BOOLEAN,
	dim_not_with_team BOOLEAN,
	m_minutes REAL,
	m_fgm INTEGER,
	m_fga INTEGER,
	m_fg3m INTEGER,
	m_fg3a INTEGER,
	m_ftm INTEGER,
	m_fta INTEGER,
	m_oreb INTEGER,
	m_dreb INTEGER,
	m_reb INTEGER,
	m_ast INTEGER,
	m_stl INTEGER,
	m_blk INTEGER,
	m_turnovers INTEGER,
	m_pf INTEGER,
	m_pts INTEGER,
	m_plus_minus INTEGER
)

-- Insert deduplicated records into the fact table
INSERT INTO fct_game_details
WITH deduped AS (
	SELECT
		g.game_date_est,
		g.season,
		g.home_team_id,
		g.visitor_team_id,
		gd.*,
		ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.team_id, gd.player_id ORDER BY g.game_date_est) AS row_num
	FROM game_details gd
		JOIN games g on gd.game_id = g.game_id
)
SELECT 
	game_date_est AS dim_game_date,
	season AS dim_season,
	team_id AS dim_team_id,
	player_id AS dim_player_id,
	player_name AS dim_player_season,
	start_position AS dim_start_position,
	team_id = home_team_id AS dim_is_playing_at_home,
	COALESCE(POSITION('DNP' in comment), 0) > 0 AS dim_did_not_play,
	COALESCE(POSITION('DND' in comment), 0) > 0 AS dim_did_not_dress,
	COALESCE(POSITION('NWT' in comment), 0) > 0 AS dim_not_with_team,
	CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL)/ 60 AS minutes, 
	fgm AS m_fgm, 
	fga AS m_fga,
	fg3m AS m_fg3m ,
	fg3a AS m_fg3a,
	ftm AS m_ftm,
	fta AS m_fta,
	oreb AS m_oreb,
	dreb AS m_dreb,
	reb AS m_reb,
	ast AS m_ast,
	stl AS m_stl,
	blk AS m_blk,
	"TO" AS turnovers,
	pf AS m_pf,
	pts AS m_pts,
	plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1

-- Example query on the new fact table that shows players with the highest percentage of games bailed out of
SELECT
	dim_player_name,
	COUNT(1) AS num_games,
	COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS bailed_num,
	CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS REAL) / COUNT(1) AS bail_pct
FROM fct_game_details
GROUP BY 1
ORDER BY 4 DESC;