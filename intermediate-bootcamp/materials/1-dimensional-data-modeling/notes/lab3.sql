CREATE TYPE vertex_type AS ENUM ('player', 'team', 'game');

CREATE TYPE edge_type AS ENUM (
  'plays_against',
  'shares_team',
  'plays_in',
  'plays_on'
);

CREATE TABLE vertices (
  identifier TEXT,
  type        vertex_type,
  properties  JSON,
  PRIMARY KEY (identifier, type)
);

CREATE TABLE edges (
  subject_identifier TEXT,
  subject_type       vertex_type,
  object_identifier  TEXT,
  object_type        vertex_type,
  edge_type          edge_type,
  properties         JSON,
  PRIMARY KEY (
    subject_identifier,
    subject_type,
    object_identifier,
    object_type,
    edge_type
  )
);

INSERT INTO vertices (identifier, type, properties)
SELECT
  game_id AS identifier,
  'game'::vertex_type AS type,
  json_build_object(
    'pts_home', pts_home,
    'pts_away', pts_away,
    'winning_team',
      CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
  ) AS properties
FROM games;

WITH players_agg AS (
  SELECT
    player_id                         AS identifier,
    MAX(player_name)                  AS player_name,      -- pick one name
    COUNT(1)                          AS number_of_games,
    SUM(pts)                          AS total_points,
    ARRAY_AGG(DISTINCT team_id)       AS teams
  FROM game_details
  GROUP BY player_id
)
INSERT INTO vertices (identifier, type, properties)
SELECT
  identifier,
  'player'::vertex_type AS type,
  json_build_object(
    'player_name',      player_name,
    'number_of_games',  number_of_games,
    'total_points',     total_points,
    'teams',            teams
  ) AS properties
FROM players_agg;

WITH teams_deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY team_id) AS row_num
  FROM teams
)
INSERT INTO vertices (identifier, type, properties)
SELECT
  team_id AS identifier,
  'team'::vertex_type AS type,
  json_build_object(
    'abbreviation', abbreviation,
    'nickname',     nickname,
    'city',         city,
    'arena',        arena,
    'year_founded', yearfounded
  ) AS properties
FROM teams_deduped
WHERE row_num = 1;

WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY player_id, game_id ORDER BY player_id, game_id) AS row_num
  FROM game_details
)
INSERT INTO edges (
  subject_identifier, subject_type,
  object_identifier,  object_type,
  edge_type,
  properties
)
SELECT
  player_id                 AS subject_identifier,
  'player'::vertex_type     AS subject_type,
  game_id                   AS object_identifier,
  'game'::vertex_type       AS object_type,
  'plays_in'::edge_type     AS edge_type,
  json_build_object(
    'start_position',       start_position,
    'pts',                  pts,
    'team_id',              team_id,
    'team_abbreviation',    team_abbreviation
  ) AS properties
FROM deduped
WHERE row_num = 1;

SELECT 
  v.properties->>'player_name',
  MAX(CAST(e.properties->>'pts' AS INTEGER))
FROM vertices v
JOIN edges e
  ON e.subject_identifier = v.identifier
 AND e.subject_type = v.type
GROUP BY 1
ORDER BY 2 DESC;

INSERT INTO edges
WITH deduped AS (
  SELECT *, row_number() OVER (PARTITION BY player_id, game_id) AS row_num
  FROM game_details
),
filtered AS (
  SELECT * FROM deduped
  WHERE row_num = 1
),
aggregated AS (
  SELECT
    f1.player_id AS subject_player_id,
    f2.player_id AS object_player_id,
    CASE WHEN f1.team_abbreviation = f2.team_abbreviation
         THEN 'shares_team'::edge_type
         ELSE 'plays_against'::edge_type
    END AS edge_type,
    MAX(f1.player_name) AS subject_player_name,
    MAX(f2.player_name) AS object_player_name,
    COUNT(1) AS num_games,
    SUM(f1.pts) AS subject_points,
    SUM(f2.pts) AS object_points
  FROM filtered f1
  JOIN filtered f2
    ON f1.game_id = f2.game_id
   AND f1.player_id <> f2.player_id
  WHERE f1.player_id > f2.player_id
  GROUP BY
    f1.player_id,
    f1.player_name,
    f2.player_id,
    f2.player_name,
    CASE WHEN f1.team_abbreviation = f2.team_abbreviation
         THEN 'shares_team'::edge_type
         ELSE 'plays_against'::edge_type
    END
)
SELECT
  subject_player_id AS subject_identifier,
  'player'::vertex_type AS subject_type,
  object_player_id AS object_identifier,
  'player'::vertex_type AS object_type,
  edge_type AS edge_type,
  json_build_object(
    'num_games',       num_games,
    'subject_points',  subject_points,
    'object_points',   object_points
  )
FROM aggregated;

SELECT 
  v.properties->>'player_name',
  e.object_identifier,
  CAST(v.properties->>'number_of_games' AS REAL) /
    CASE WHEN CAST(v.properties->>'total_points' AS REAL) = 0 THEN 1 
         ELSE CAST(v.properties->>'total_points' AS REAL) END,
  e.properties->>'subject_points',
  e.properties->>'num_games'
FROM vertices v
JOIN edges e
  ON v.identifier = e.subject_identifier
 AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type;