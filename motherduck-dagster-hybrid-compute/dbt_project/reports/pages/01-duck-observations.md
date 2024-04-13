### Duck Observations


There were <strong><Value data={duck_counts} value='n_checklists' /></strong> checklists, <strong><Value data={duck_counts} value='n_observations' /></strong> observations, and <strong><Value data={duck_counts} value='total_species' /></strong> total species of Ducks.

Most observations occur on Saturdays -- the FeederWatch dataset is seasonal, operating from November to April of each year.

<CalendarHeatmap 
    data={duck_counts_recent} 
    date=obs_date 
    value=n_observations 
    title="Calendar Heatmap"
    subtitle="Daily Observations"
/>

```sql duck_counts
select

    obs_date,
    date_trunc('year', obs_date) as obs_year,
    count(distinct checklist_id) as n_checklists,
    count(distinct observation_id) as n_observations,
    sum(species_count) as total_species

from dagster_hybrid_demo.all_ducks
group by all
order by 1 desc
```

```sql duck_counts_recent
select

    obs_date,
    date_trunc('year', obs_date) as obs_year,
    count(distinct checklist_id) as n_checklists,
    count(distinct observation_id) as n_observations,
    sum(species_count) as total_species

from dagster_hybrid_demo.all_ducks
where obs_date > '2020-01-01'
group by all
order by 1 desc
```
