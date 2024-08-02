---
title:  Data Engineering Case Study
---

# Task 1

You receive data in a set of new nightly batch pipelines: Sales, Loading and Pax.
This data should be provided to two different teams: A team of data scientists and a team of data analysts.
Build a pipeline which delivers the data so that it fits the needs of your stakeholders best. You are free to choose a tech stack and approach, you see suitable to fulfill the requirements.
Deliver your code and documentation, include all relevant files, instructions, and results.


## Data

The data consists of sales (in-flight sales), pax (passenger information), and loading (goods loading slip information) data for the fictional zeroG Airline for the time frame of March and April 2019.
The data comes from different teams and different systems every night in the given format. For simplicity, we aggregated it here on a monthly basis.


## Requirements

### Data Scientists

The goal of the data scientists is to train a model to predict the food consumption for a flight, based on passenger number per flight and the number of sold items on the flight.

### Data Analysts

The data analysts want to display the relevant data, including the number of passengers, the number of sold items, and the number of loaded items per flight via Tableau to the stakeholders.

# Task 2

Imagine the pipeline from Task 1 delivers the content of all airlines within the whole LHG instead of only the flights of the zeroG Airline.
As the data has a very high volume it is served to you in microbatches, instead of nightly batches.

How would this change your approach?
Deliver a short architectual diagram and description which outlines your planned solution. As with task 1, you are free to chose the tech stack which you think fits the task best.

