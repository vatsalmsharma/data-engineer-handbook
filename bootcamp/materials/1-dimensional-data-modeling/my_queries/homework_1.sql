-- 1. DDL for actors table: Create a DDL for an actors table 

create type films_struct as (
	film text,
	votes integer,
	rating real,
	filmid text
);

create type performance_quality as
	ENUM (
		'star', 'good', 'average', 'bad'
	);

create table actors(
	actor 			text,
	films 			films_struct[],
	quality_class 	performance_quality,
	is_active		boolean,
	current_year	integer
    , Primary Key(actor, current_year)
);


-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------

-- 2. Cumulative table generation query: Write a query that populates the actors table one year at a time.
insert into actors
with last_year as (
	select *
	from actors 
	where current_year = 1969
),
current_year as (
	select actor,
	year,
	Array_Agg(
			Row(
				film,
				votes,
				rating,
				filmid
			) :: films_struct
		) as films,
	avg(rating) as avg_rating
	from actor_films
	where year = 1970
	group by actor, year
)
select 
	coalesce(cy.actor, ly.actor) as actor,
	case
		when ly.films is NULL
			then cy.films
		when cy.films is Not NULL
			then array_cat(ly.films, cy.films)
		else
			ly.films
	end as films,
	case
		when cy.year is Not Null then
			case
				when cy.avg_rating > 8 then 'star'
				when cy.avg_rating > 7 then 'good'
				when cy.avg_rating > 6 then 'average'
				else	'bad'
			end :: performance_quality
		else
			ly.quality_class
	end as quality_class,
	case
		when cy.year is Not Null then True
		else	False
	end as is_active,
	coalesce(cy.year, ly.current_year + 1) as current_year
from current_year cy
	full outer join last_year ly
		on cy.actor = ly.actor;

-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------

-- 3. DDL for actors_history_scd table 
create table actors_history_scd(
	actor 			text,
	quality_class 	performance_quality,
	is_active		boolean,
	start_date		integer,
	end_date		integer,
	current_year	integer
	, Primary Key(actor, start_date)
);


-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
-- 4. Backfill query for actors_history_scd: Write a "backfill" query that can populate the entire actors_history_scd table in a single query.
-- Doing upto 1974

insert into actors_history_scd
with actor_prev as (
	select actor,
		quality_class,
		lag(quality_class, 1) over (partition by actor order by current_year) as prev_quality_class,
		is_active,
		lag(is_active, 1) over (partition by actor order by current_year) as prev_is_active,
		current_year
	from actors
	where current_year <= 1974
),
actor_change_ind as (
	select actor,
		quality_class,
		is_active,
		case
				when quality_class <> prev_quality_class 
					then 1
				when is_active <> prev_is_active 
					then 1
				when prev_quality_class is NULL 
					then 0
				when prev_is_active is NULL
					then 0
			else 0
		end as change_ind,
		current_year
	from actor_prev
)
, actor_streak as (
	select actor,
			quality_class,
			is_active,
			sum(change_ind) over(partition by actor order by current_year) as streak,
			current_year
	from actor_change_ind
)
select actor,
		quality_class,
		is_active,
--		streak,
		min(current_year) as start_date,
		max(current_year) as end_date,
		1974 as current_year
from actor_streak
group by actor,
		quality_class,
		is_active
		,streak
order by actor, start_date;


-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
-- 5. Incremental query for actors_history_scd: Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table.
-- Doing for year 1975

create type scd_type as (
	quality_class 	performance_quality,
	is_active		boolean,
	start_date		integer,
	end_date		integer
);

with last_year_scd as (
	select * from actors_history_scd
	where current_year = 1974
	and end_date = 1974
),
current_year_data as (
	select * from actors 
	where current_year = 1975
),
historic_scd as (
	select actor,
		quality_class,
		is_active,
		start_date,
		end_date	
	from actors_history_scd
	where current_year = 1974
	and end_date < 1974
),
unchanged_records as (
	select	cy.actor,
			cy.quality_class,
			cy.is_active,
			ly.start_date,
			cy.current_year as end_date
	from current_year_data cy
	JOIN last_year_scd ly
		on cy.actor = ly.actor
	where cy.quality_class = ly.quality_class
		and cy.is_active = ly.is_active
),
changed_records as (
	select	cy.actor,
			UNNEST (ARRAY[
				ROW( ly.quality_class, ly.is_active, ly.start_date, ly.end_date) :: scd_type,
				ROW( cy.quality_class, cy.is_active, cy.current_year, cy.current_year) :: scd_type
			]) as records
	from current_year_data cy
	JOIN last_year_scd ly
		on cy.actor = ly.actor
	where (
			cy.quality_class <> ly.quality_class 
				OR
			cy.is_active <> ly.is_active
		  )
),
unnested_changed_records as (
	select actor,
		(records :: scd_type).*
	from changed_records
),
new_records as (
	select cy.actor,
		cy.quality_class,
		cy.is_active,
		cy.current_year as start_date,
		cy.current_year as end_date
	from current_year_data cy 
		left join last_year_scd ly
			on cy.actor = ly.actor
		where ly.actor is null
)
select * from (
	select h.*, 1975 as current_year from historic_scd as h
	union all 
	select uc.*, 1975 as current_year from unchanged_records as uc
	union all 
	select c.*, 1975 as current_year from unnested_changed_records as c
	union all 
	select n.*, 1975 as current_year from new_records as n
	) ccc
order by actor, start_date
;