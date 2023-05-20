with

users as (

		select * from {{ ref('stg_greenery__users') }}

)

, final as (

		select
				count(distinct user_guid) as number_of_users

		from users

)

select * from final