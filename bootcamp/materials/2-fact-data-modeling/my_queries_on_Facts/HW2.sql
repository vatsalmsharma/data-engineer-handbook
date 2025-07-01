-- HW2 : Week 2 Fact Data Modeling

----------------------------------------------------------------------------------------------------------------------
-- Query #1 
-- A query to deduplicate game_details from Day 1 so there's no duplicates

with cte as (
			select 
			row_number() over(partition by game_id, team_id, player_id) as rn
			, *
			from game_details
)
select 
"plus_minus"
,"team_id"
,"ast"
,"stl"
,"blk"
,"TO"
,"pf"
,"pts"
,"game_id"
,"player_id"
,"fgm"
,"fga"
,"fg_pct"
,"fg3m"
,"fg3a"
,"fg3_pct"
,"ftm"
,"fta"
,"ft_pct"
,"oreb"
,"dreb"
,"reb"
,"team_abbreviation"
,"team_city"
,"min"
,"player_name"
,"nickname"
,"start_position"
,"comment"
from cte
where rn = 1;

----------------------------------------------------------------------------------------------------------------------
-- Query #2

/*
A DDL for an user_devices_cumulated table that has:

    a device_activity_datelist which tracks a users active days by browser_type
    data type here should look similar to MAP<STRING, ARRAY[DATE]>
    or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)
*/

create table user_devices_cumulated(
	user_id 					TEXT,
	device_activity_datelist 	DATE[],
	browser_type 				TEXT,
	record_date					DATE,
	Primary Key (user_id, browser_type, record_date)
);


----------------------------------------------------------------------------------------------------------------------
-- Query #3

-- A cumulative query to generate device_activity_datelist from events
-- SEED => yesterday as '2022-12-31'

insert into user_devices_cumulated
with yesterday as (
			select 
				user_id,
				device_activity_datelist,
				browser_type,
				record_date	
			from user_devices_cumulated
			where record_date = '2023-01-01'
),
today as (
			select 
			distinct 
				cast (e.user_id as TEXT) as user_id, 
				d.browser_type,
				date (cast (event_time as timestamp)) as record_date
			from events e
			inner join devices d
				on e.device_id = d.device_id
			where date (cast (event_time as timestamp)) = date('2023-01-02')
				and e.user_id is not NULL
)
select 
	coalesce(t.user_id, y.user_id) as user_id,
	case 
		when y.record_date is null
			then ARRAY[t.record_date]
		when t.record_date is null
			then y.device_activity_datelist
		else
			ARRAY[t.record_date] || y.device_activity_datelist
	end as device_activity_datelist,
	coalesce(t.browser_type, y.browser_type) as browser_type,
	coalesce(t.record_date, y.record_date + 1) as record_date
from today t
	full outer join yesterday y 
		on t.user_id = y.user_id and t.browser_type = y.browser_type;

----------------------------------------------------------------------------------------------------------------------
-- Query #4

-- A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column
with user_browser as (
						select *
						from user_devices_cumulated
						where record_date = date('2023-01-31')
					  ),
series as (
			select *
			from generate_series(
									date('2023-01-01'), 
									date('2023-01-31'), 
									Interval '1 day'
								) as series_date
), placeholder_int as (
						select 
						date(s.series_date)
						-- , device_activity_datelist @> ARRAY[date(series_date)]
						-- , record_date - date(s.series_date) as dates_since_active
						, case 
							when device_activity_datelist @> ARRAY[date(series_date)]
								then cast (
											pow(2, 32 - (record_date - date(s.series_date)))
											as BIGINT
										  ) 
							else
								0
						  end as placeholder_int_value
						,u.* 
						from user_browser u
							cross join series s
)
select 
user_id,
-- sum(placeholder_int_value),
cast(cast(sum(placeholder_int_value) as BIGINT) as BIT(32)) as datelist_int
-- , BIT_COUNT(
-- 		cast(cast(sum(placeholder_int_value) as BIGINT) as BIT(32))
-- 		)
from placeholder_int
group by user_id;


----------------------------------------------------------------------------------------------------------------------
-- Query #5

--  DDL for hosts_cumulated table : a host_activity_datelist which logs to see which dates each host is experiencing any activity

create table hosts_cumulated(
	host 				    	TEXT,
	host_activity_datelist  	DATE[],
	record_date					DATE,
	Primary Key (host, record_date)
);


----------------------------------------------------------------------------------------------------------------------
-- Query #6
-- The incremental query to generate host_activity_datelist

-- Initial Load
INSERT INTO hosts_cumulated
with host_events as (
					select 
					distinct 
						host, 
						cast(cast(event_time as timestamp) as date) as record_date
					from events
					)
select host, 
ARRAY[record_date] as host_activity_datelist,
record_date
from host_events
where record_date = date('2023-01-01');

-- Incremental Load
with yesterday as (
					select 
						host,
						host_activity_datelist
					from hosts_cumulated
					where record_date = date('2023-01-07')
), today as (
					select 
					distinct 
						host, 
						cast(cast(event_time as timestamp) as date) as record_date
					from events
					where cast(cast(event_time as timestamp) as date) = date('2023-01-08')
), historic_records as (
					select 
						host,
						host_activity_datelist,
						record_date
					from hosts_cumulated
					where record_date < date('2023-01-08')
), unchanged_records as (
					select 
						t.host,
						ARRAY[t.record_date] || y.host_activity_datelist as host_activity_datelist,
						t.record_date
 					from today t
						Inner Join yesterday y 
							on t.host = y.host
),
only_yesterday_records as (
					select 
						y.host,
						y.host_activity_datelist,
						y.host_activity_datelist[1] + 1 as record_date
					from yesterday y
						left join today t 
							on y.host = t.host 
					where t.host is NULL
),
only_today_records as (
					select 
						t.host,
						ARRAY[t.record_date],
						t.record_date
					from today t 
						left join yesterday y 
							on t.host = y.host
					where y.host is NULL
)
select * from (
	select * from historic_records
	union all
	select * from unchanged_records
	union all
	select * from only_yesterday_records
	union all
	select * from only_today_records
) ccc
order by host, record_date;  

----------------------------------------------------------------------------------------------------------------------
-- Query #7

/*
A monthly, reduced fact table DDL host_activity_reduced
    month
    host
    hit_array - think COUNT(1)
    unique_visitors array - think COUNT(DISTINCT user_id)
*/

create table host_activity_reduced(
	month 					date,
	host 					text,
	hit_array 				INTEGER[],
	unique_visitors_array 	INTEGER[],
	Primary key (month, host)
);

----------------------------------------------------------------------------------------------------------------------
-- Query #8

-- An incremental query that loads host_activity_reduced : day-by-day

insert into host_activity_reduced
with start_of_month as (
	select * 
	from host_activity_reduced
	where month = date('2023-01-01') -- start of the month
),
today as (
			select 
			cast (cast (event_time as timestamp)  as date) as curr_date,
			host,
			count(1) as host_hit_count,
			count(distinct user_id) as user_count
			from events e
			where cast (cast(event_time as timestamp) as date) = date('2023-01-02')
			group by cast (cast (event_time as timestamp)  as date), host
)
select 
	cast(coalesce( DATE_TRUNC('month', t.curr_date), som.month ) as date) as month,
	coalesce(t.host, som.host) as host,
	case 
		when som.hit_array is NOT NULL
			then som.hit_array || ARRAY[ coalesce(t.host_hit_count, 0)]
		when som.hit_array is NULL
			then ARRAY_FILL(0, ARRAY[COALESCE (t.curr_date - DATE(DATE_TRUNC('month', t.curr_date)), 0)]) 
                || ARRAY[COALESCE(t.host_hit_count,0)]
	end as hit_array,
	case 
		when som.unique_visitors_array is NOT NULL
			then som.unique_visitors_array || ARRAY[ coalesce(t.user_count, 0)]
		when som.unique_visitors_array is NULL
			then ARRAY_FILL(0, ARRAY[COALESCE (t.curr_date - DATE(DATE_TRUNC('month', t.curr_date)), 0)]) 
                || ARRAY[COALESCE(t.user_count,0)]
	end as unique_visitors_array
from today t 
full outer join start_of_month som
	on t.host = som.host
ON CONFLICT (host, month)
DO 
    UPDATE SET hit_array = EXCLUDED.hit_array,
    			unique_visitors_array = EXCLUDED.unique_visitors_array	
	;


