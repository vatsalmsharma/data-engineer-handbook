-- Finding grain of the record and if there are any duplictes
select 
 game_id, team_id, player_id, count(1)
from game_details
group by game_id, team_id, player_id
having count(1) > 1;

-- To dedupe, row_numer() over (partition by <grain of table>) 
with deduped as (
	select 
	 row_number() over (partition by game_id, team_id, player_id) as rownum,
	 *
	from game_details
)
select * from deduped 
where rownum = 1
;

