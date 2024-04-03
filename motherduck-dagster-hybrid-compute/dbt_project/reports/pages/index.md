# The Duck Details


<div style="display: flex; justify-content: center;">
  <img src="/logos/dagster.svg" style="flex: 1; max-width: 72px;">
  <img src="/logos/motherduck.svg" style="flex: 1; max-width: 72px;">
</div>


## Project Feederwatch

Project FeederWatch turns your love of feeding birds into scientific discoveries. FeederWatch is a November-April survey of birds that visit backyards, nature centers, community areas, and other locales in North America. Count your birds for as long as you like on days of your choosing, then enter your counts online. Your counts allow you to track what is happening to birds around your home and to contribute to a continental data-set of bird distribution and abundance.

- [feederwatch.org/explore/raw-dataset-requests](https://feederwatch.org/explore/raw-dataset-requests/)

> Note that raw data files are large (> 1.8 million checklists) and require proficiency in statistical software (e.g. SAS or R) or advanced database tools (e.g. MySQL, Microsoft Access). Project FeederWatch does not have the staff available to assist with these tools or to create custom subsets of the raw data.

## Dagster Asset Lineage

![Dagster Asset Lineage](/asset-graph.png)

## Visualizations


```sql top_ducks_annually
select

    concat('/flags/', country, '.png') as flag,
    year,
    bird_name,
    country,
    region,
    species_count

from motherduck.top_ducks_by_year
order by
  year desc,
  species_count desc
```

<DataTable data="{top_ducks_annually}" search="true" rows="10" >
  <Column id="flag" contentType=image height=30px align=center />
  <Column id="year" align="left" />
  <Column id="bird_name" align="left" />
  <Column id="region" align="right" />
  <Column id="species_count" align="right" contentType="colorscale" scaleColor="green"/>

</DataTable>

```sql duck_counts
select

    date_trunc('year', obs_date) as obs_year,
    count(distinct checklist_id) as n_checklists,
    count(distinct observation_id) as n_observations,
    sum(species_count) as total_species

from motherduck.all_ducks
group by all
order by 1 desc
```

<BigValue
    data={duck_counts}
    value='n_checklists'
    title='Num Checklists'
    sparkline='obs_year'
    width=250
/>

<BigValue
    data={duck_counts}
    value='n_observations'
    title='Num Observations'
    sparkline='obs_year'
    width=250
/>

<BigValue
    data={duck_counts}
    value='total_species'
    title='Num Species'
    sparkline='obs_year'
    width=250
/>

---


## Duck Observations

<BarChart 
    data={duck_counts} 
    x=obs_year
    y=n_observations 
/>


## Regional Metrics

By far, the most duck observations occur in [Florida](https://en.wikipedia.org/wiki/Florida), either these people have a lot of ducks or enough time on their hands to report them to Feeder Watch.


```sql ducks_by_state

select

    state,
    total_species_count

from motherduck.top_ducks_by_state

where obs_year = 2023
```

<USMap
    data={ducks_by_state}
    state=state
    value=total_species_count
    legend=true
    abbreviations=true
    colorScale=red
/>

## Most Uncommon Species

```sql most_rare_species

select
  bird_name,
  sum(species_count) as count
from motherduck.top_ducks_by_year
where
  lower(bird_name) not like '%sp.%'
  and lower(bird_name) not like '%hybrid%'
group by bird_name
having count < 25

```

The most uncommon Duck observations excluding hybrids breeds include:

 1. The <strong>Long Tailed Duck</strong>

> The long-tailed duck (Clangula hyemalis) or coween,[2] formerly known as the oldsquaw, is a medium-sized sea duck that breeds in the tundra and taiga regions of the arctic and winters along the northern coastlines of the Atlantic and Pacific Oceans. It is the only member of the genus Clangula. [[more info]](https://en.wikipedia.org/wiki/Long-tailed_duck)

![Long Tailed Duck](/ducks/long-tailed-duck.jpg)

 2. The <strong>Mandarin Duck</strong> 

> The mandarin duck (Aix galericulata) is a perching duck species native to the East Palearctic. It is sexually dimorphic, males showing a dramatic difference from the females.[3] It is medium-sized, at 41–49 cm (16–19 in) long with a 65–75 cm (26–30 in) wingspan. It is closely related to the North American wood duck, the only other member of the genus Aix. 'Aix' is an Ancient Greek word which was used by Aristotle to refer to an unknown diving bird, and 'galericulata' is the Latin for a wig, derived from galerum, a cap or bonnet.[4] Outside of its native range, the mandarin duck has a large introduced population in the British Isles and Western Europe, with additional smaller introductions in North America. [[more info]](https://en.wikipedia.org/wiki/Mandarin_duck)

![Mandarin Duck](/ducks/mandarin-duck.jpg)

 3. The <strong>Muscovy Duck</strong> 

> The Muscovy duck (Cairina moschata) is a duck native to the Americas, from the Rio Grande Valley of Texas and Mexico south to Argentina and Uruguay. Feral Muscovy ducks are found in New Zealand, Australia, and in Central and Eastern Europe. Small wild and feral breeding populations have also established themselves in the United States, particularly in Florida, Louisiana, Massachusetts, the Big Island of Hawaii, as well as in many other parts of North America, including southern Canada. [[more info]](https://en.wikipedia.org/wiki/Muscovy_duck)

![Muscovy Duck](/ducks/muscovy-duck.jpg)
