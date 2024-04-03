select

    region as state,
    year(date_trunc('year', obs_date)) as obs_year,
    sum(species_count) as total_species_count

from {{ ref('all_birds') }}
where

  lower(bird_name) like '%duck%'
  and country = 'US'

group by all
order by 1 desc
