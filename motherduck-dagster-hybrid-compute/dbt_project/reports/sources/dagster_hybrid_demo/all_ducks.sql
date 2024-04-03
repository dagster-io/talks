select

  checklist_id,
  observation_id,
  loc_id,
  latitude,
  longitude,
  country,
  region,
  obs_date,
  species_code,
  species_count,
  is_valid,
  is_reviewed,
  bird_name,
  nearby_feeders,
  population_city,
  survey_site_size

from dagster_hybrid_demo.main.all_birds

where
  lower(bird_name) like '%duck%'
