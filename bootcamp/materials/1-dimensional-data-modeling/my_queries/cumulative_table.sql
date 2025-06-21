select * from player_seasons
limit 10;

-- Creating custom STRUCT TYPE
create type season_stat as (
	season integer,
	gp integer,
	pts real, 
	reb real,
	ast real
);

-- drop type season_stat;

create table player(
-- following fields are constant about a player
	player_name text,
	height text,
	country text,
	draft_year text,
-- following fields are the ones that will change
	season_stats season_stat[], -- Note here we are defining the datatype as ARRAY of STRUCT
	current_season integer,
	PRIMARY KEY (player_name, current_season)
);

-- drop table player;

-- Cumulative Table design 

-- Find the minimum 'season' year accross the data. 
select min(season) from player_seasons; -- 1996

For Cumulative table, its FULL OUTER JOIN (essentially combination of both LEFT and RIGHT JOIN)
between 'yesterday' and 'today' data.

The starting query is called 'SEED Query'
In this case, 'yesterday' will be 1995 and 'today' 1996

-- SEED Query 
with yesterday as (
	select 	* from player
	where current_season = 1995
),
today as (
	select * from player_seasons
	where season = 1996
)
select 
	-- *
	coalesce(t.player_name, y.player_name) as player_name, 
	coalesce(t.height, y.height) as height,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	case 
		-- when its new player, i.e., player doesn't exist yesterday
		when y.season_stats is NULL
			then Array[
						ROW(
							t.season,
							t.gp,
							t.pts, 
							t.reb,
							t.ast
						) :: season_stat
			]
		-- Existing player, we want to append the new stats to yesterday's stats	
		when t.season is not NULL
			then y.season_stats || Array[
						ROW(
							t.season,
							t.gp,
							t.pts, 
							t.reb,
							t.ast
						) :: season_stat
			]
		-- if the player is retired, we want to hold on the distory without adding bunch of NULLs
		else
			y.season_stats
		end as season_stats,
		coalesce (t.season, y.current_season + 1) as current_season
from today t
	full outer join yesterday y
		on t.player_name = y.player_name;

-- Finally Building the Cumulative table
-- Start with 'yesterday as 1995' and today as 1996 
-- then re-sum same query by incrementing both yesterday and today years by 1

insert into player
with yesterday as (
	select 	* from player
	where current_season = 1996
),
today as (
	select * from player_seasons
	where season = 1997
)
select 
	-- *
	coalesce(t.player_name, y.player_name) as player_name, 
	coalesce(t.height, y.height) as height,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	case 
		-- when its new player, i.e., player doesn't exist yesterday
		when y.season_stats is NULL
			then Array[
						ROW(
							t.season,
							t.gp,
							t.pts, 
							t.reb,
							t.ast
						) :: season_stat
			]
		-- Existing player, we want to append the new stats to yesterday's stats	
		when t.season is not NULL
			then y.season_stats || Array[
						ROW(
							t.season,
							t.gp,
							t.pts, 
							t.reb,
							t.ast
						) :: season_stat
			]
		-- if the player is retired, we want to hold on the distory without adding bunch of NULLs
		else
			y.season_stats
		end as season_stats,
		coalesce (t.season, y.current_season + 1) as current_season
from today t
	full outer join yesterday y
		on t.player_name = y.player_name;


-- Power of Cumulative table

select * 
from player
where current_season = 2001
and player_name = 'Michael Jordan';

-- Season Stats
-- "{""(1996,82,29.6,5.9,4.3)"",""(1997,82,28.7,5.8,3.5)"",""(2001,60,22.9,5.7,5.2)""}"

select player_name,
	unnest(season_stats)::season_stat
from player
where current_season = 2001
and player_name = 'Michael Jordan';
/*
"Michael Jordan"	"(1996,82,29.6,5.9,4.3)"
"Michael Jordan"	"(1997,82,28.7,5.8,3.5)"
"Michael Jordan"	"(2001,60,22.9,5.7,5.2)"
*/


with unnested as (
select player_name,
	unnest(season_stats)::season_stat as un
from player
where current_season = 2001
and player_name = 'Michael Jordan'
)
select player_name, (un::season_stat).*
from unnested;

/*
"Michael Jordan"	1996	82	29.6	5.9	4.3
"Michael Jordan"	1997	82	28.7	5.8	3.5
"Michael Jordan"	2001	60	22.9	5.7	5.2
*/


create type scoring_class as 
	enum('star', 'good', 'average', 'bad');

-- Adding 2 more columns to PLAYER	table 
-- drop table player;

create table player(
-- following fields are constant about a player
	player_name text,
	height text,
	country text,
	draft_year text,
-- following fields are the ones that will change
	season_stats season_stat[], -- Note here we are defining the datatype as ARRAY of STRUCT
	scoring_class scoring_class,
	years_since_last_season integer,
	current_season integer,
	PRIMARY KEY (player_name, current_season)
);

-- Insert data starting from 'SEED Query' onwards

insert into player
with yesterday as (
	select 	* from player
	where current_season = 2000
),
today as (
	select * from player_seasons
	where season = 2001
)
select 
	-- *
	coalesce(t.player_name, y.player_name) as player_name, 
	coalesce(t.height, y.height) as height,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	case 
		-- when its new player, i.e., player doesn't exist yesterday
		when y.season_stats is NULL
			then Array[
						ROW(
							t.season,
							t.gp,
							t.pts, 
							t.reb,
							t.ast
						) :: season_stat
			]
		-- Existing player, we want to append the new stats to yesterday's stats	
		when t.season is not NULL
			then y.season_stats || Array[
						ROW(
							t.season,
							t.gp,
							t.pts, 
							t.reb,
							t.ast
						) :: season_stat
			]
		-- if the player is retired, we want to hold on the distory without adding bunch of NULLs
		else
			y.season_stats
		end as season_stats,
		case
			when t.season is not null then
				case
					when t.pts > 20 then 'star'
					when t.pts > 15 then 'good'
					when t.pts > 10 then 'average'
					else 'bad'
				end :: scoring_class
			else
				y.scoring_class
		end as scoring_class,					
		case 
				when t.season is not null then 0
			else
				y.years_since_last_season + 1
		end as years_since_last_season,
		coalesce (t.season, y.current_season + 1) as current_season
from today t
	full outer join yesterday y
		on t.player_name = y.player_name;


-- Let's do analytics
-- What are the Season-Stats of 2001 and the first-season of each player 
select player_name,
	season_stats[1] as first_season,
	season_stats[cardinality(season_stats)] as latest_season
from player
where current_season = 2001;

-- What is the improvement in points from first season to 2001?
select player_name, 
(season_stats[cardinality(season_stats)] :: season_stat).pts as latest_points,
case 
	when (season_stats[1] :: season_stat).pts = 0 then 1 
	else (season_stats[1] :: season_stat).pts
end as first_season_points,
(season_stats[cardinality(season_stats)] :: season_stat).pts /
(case 
	when (season_stats[1] :: season_stat).pts = 0 then 1 
	else (season_stats[1] :: season_stat).pts
end ) as improvement
from player
where current_season = 2001
order by 4 desc
;