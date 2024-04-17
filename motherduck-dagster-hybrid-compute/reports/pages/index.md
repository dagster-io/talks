# The Duck Details with Dagster, MotherDuck, and Evidence


- Orchestration with [Dagster](https://dagster.io/)
- [MotherDuck](https://motherduck.com/) as the data warehouse
- And [Evidence](https://evidence.dev/) for visualizations

<div style="display: flex; justify-content: center;">
  <img src="/logos/dagster.svg" style="flex: 1; max-width: 52px; margin-right: 10px;">
  <img src="/logos/motherduck.svg" style="flex: 1; max-width: 52px; margin-right: 10px;">
  <img src="/logos/evidence.svg" style="flex: 1; max-width: 42px;">
</div>

---

## Project Feederwatch

Project FeederWatch turns your love of feeding birds into scientific discoveries. FeederWatch is a November-April survey of birds that visit backyards, nature centers, community areas, and other locales in North America. Count your birds for as long as you like on days of your choosing, then enter your counts online. Your counts allow you to track what is happening to birds around your home and to contribute to a continental data-set of bird distribution and abundance.

- [feederwatch.org/explore/raw-dataset-requests](https://feederwatch.org/explore/raw-dataset-requests/)

> Note that raw data files are large (> 1.8 million checklists) and require proficiency in statistical software (e.g. SAS or R) or advanced database tools (e.g. MySQL, Microsoft Access). Project FeederWatch does not have the staff available to assist with these tools or to create custom subsets of the raw data.

## Dagster Asset Lineage

![Dagster Asset Lineage](/asset-graph.png)
