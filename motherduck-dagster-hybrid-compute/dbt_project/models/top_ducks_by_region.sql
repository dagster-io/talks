select

    country,
    region,
    bird_name,
    total_count

from (

    select

        country,
        region,
        bird_name,
        sum(species_count) as total_count,
        row_number() over (partition by region order by sum(species_count) desc) as rank

    from {{ ref('all_birds') }}
    where region in ('CA', 'ON', 'NY', 'PA', 'VA', 'FL')
    group by 1, 2, 3

) where rank <= 5
