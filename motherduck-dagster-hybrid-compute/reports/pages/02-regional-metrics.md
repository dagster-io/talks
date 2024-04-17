# Regional Metrics

## Top Annual Duck Species

<DataTable data="{top_ducks_annually}" search="true" rows="10" >
  <Column id="country_flag" contentType=image height=30px />
  <Column id="region_flag" contentType=image height=30px />
  <Column id="country" align=left />
  <Column id="region" align=left />
  <Column id="year" align=left />
  <Column id="bird_name" align=left />
  <Column id="species_count" align=right contentType=colorscale scaleColor=green />
</DataTable>

```sql top_ducks_annually
select

    -- original structure was going to be /flags/<country>/<region>.png, but this was causing issues :shrug:
    concat('/country-flag/', lower(country), '.png') as country_flag,
    concat('/region-flag/', lower(region), '.png') as region_flag,
    year,
    bird_name,
    country,
    region,
    species_count

from dagster_hybrid_demo.top_ducks_by_year
order by
  year desc,
  species_count desc
```

## U.S. State Heatmap

By far, the most duck observations occur in [Florida](https://en.wikipedia.org/wiki/Florida), either these people have a lot of ducks or enough time on their hands to report them to Feeder Watch.

<USMap
    data={ducks_by_state}
    state=state
    value=total_species_count
    legend=true
    abbreviations=true
    colorScale=red
    max=500
/>

```sql ducks_by_state
select

    state,
    total_species_count

from dagster_hybrid_demo.top_ducks_by_state

where obs_year = 2023
```
