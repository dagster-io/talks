# The Duck Details

## Source

Project FeederWatch turns your love of feeding birds into scientific discoveries. FeederWatch is a November-April survey of birds that visit backyards, nature centers, community areas, and other locales in North America. Count your birds for as long as you like on days of your choosing, then enter your counts online. Your counts allow you to track what is happening to birds around your home and to contribute to a continental data-set of bird distribution and abundance.

- [feederwatch.org/explore/raw-dataset-requests](https://feederwatch.org/explore/raw-dataset-requests/)

> Note that raw data files are large (> 1.8 million checklists) and require proficiency in statistical software (e.g. SAS or R) or advanced database tools (e.g. MySQL, Microsoft Access). Project FeederWatch does not have the staff available to assist with these tools or to create custom subsets of the raw data.


```sql top_ducks_annually
select
  year,
  bird_name,
  country,
  region,
  species_count
from motherduck.top_ducks_annually
where
  lower(bird_name) like '%duck%'
order by
  year desc,
  species_count desc
```

<DataTable data="{top_ducks_annually}" search="true" />

```sql duck_counts
select
    date_trunc('year', obs_date) as obs_year,
    count(distinct checklist_id) as n_checklists,
    count(distinct observation_id) as n_observations,
    sum(species_count) as total_species
from motherduck.all_ducks
where
  lower(bird_name) like '%duck%'
group by all
order by 1 desc
```

<BigValue
    data={duck_counts}
    value='n_checklists'
    title='Num Checklists'
    sparkline='obs_year'
/>

<BigValue
    data={duck_counts}
    value='n_observations'
    title='Num Observations'
    sparkline='obs_year'
/>

<BigValue
    data={duck_counts}
    value='total_species'
    title='Num Species'
    sparkline='obs_year'
/>
