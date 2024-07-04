Welcome to Henri's DBT project for the 2024 Analytics Engineer Wolt Assignment!

The assignment revolves around a business problem, the reduction of delivery radiuses, and relies on 3 base datasets (purchases, delivery_radius_log and hours).

In this DBT repository would be housed all models related to this assignment, with a basic showcase of how I would set things up.


The solutions to this assignment were designed in a context going beyond the sole end results, focusing also on infrastructure, basic modelling and the techniques that would actually be used on the job. As such, it was done as if it was a real life project, with all the setup going with it, including :
- the Redshift Cluster
- the S3 bucket storing the baes CSVs (in case of data streaming, other tools would be used)
- the whole DBT project and local development environment with DBT Core, along with very basic tests and documentation
- the Github repository housing the DBT project (here)




**Logic behind task 1 :**

The aim is to gauge the consistency of the variable (delivery radius) over time. An intuitive approach was taken :
- first identifying the radius changes over time
- building intervals of consistency over which the radius is constant and measuring their duration
- determining the radius changes that affect the default delivery radius
- fill in gaps to have a full default delivery radius table

Task 2 derives directly from task 1.



**Assumptions and design decisions**

1. **time treatment** : 
- When calculating the default radiuses in dw_default_delivery_radiuses, time difference was first calculated in minutes for better accuracy. Indeed, datediff in Redhift compares two data points using their date part, meaning date_diff(hour, '2024-01-01 01:59:59', '2024-01-01 03;00;00') = date_diff(hour, '2024-01-01 01:01:01', '2024-01-01 03;59;59') = 2, which is vastly inaccurate. 

- When computing the time intervals on which default delivery radiuses are active, because of the constraints of having them match with hourly periods, some adjustments also had to be made : using a simple function, timestamps were rounded up or down to the closest hour in dm_delivery_radius_reduction_financial_analysis. 

- One important note is that the consistency of these design decisions is very important : for the stakeholders, getting the same results for the same KPIs across different models is essential to the trust in the data quality ; for the Analytics Engineering team, it is equally as important, to get tests right, the measures, consistent across models, and keep the same standards across the repository/company.
 
2. **frontfilling the default radiuses**

- In the absence of new events in the delivery radius logs, frontfilling was implemented in the default delivery radiuses so that analysis could be done comprehensively for the last days of 2023 (delivery_area_id 5cc1b60b034adf90cd8f14dd had no data after the 26th of December, 2023). A one-month frontfill was arbitrarily designed but such decisions have to proactively be made with the business and the end user of these tables. In the absence of any indication, a frontfill up to current_date/current_timestamp could be implemented.

3. **Modelling and performance considerations**

- In terms of layers, the assumption was made that radius logs and purchases could come from different apps and different systems, in which case the DL models would be separate, but all consolidated in the DW models where standards across the different sources would be applied the the relevant entities in coordination with external stakeholders (for example, time_received was mostly used from the purchases entity, taking an "order intake" business development perspective, but from the accounting perspective, the time_delivered would be more relevant for revenue recognition).

As for performance : 
- as many models as possible would be incrementalized in the long run, to allow for high frequency refreshes.
- A lot of window functions were used in dw_default_delivery_radiuses for the computation of default delivery radiuses, as there is fundamentally a recursivity in the problem (on intervals in which the delivery radius is consistent, duration has to be summed by iterating over the subsequent timestamps). The approach taken seemed the most straightforward, is easy to audit and debug, and allows other use cases (analysis on the duration of consistency intervals, on default_radius changes etc..), but performance can definitely be a concern. With more time, another approach with a recursive CTE could be tested out, Choosing the correct sort_key and dist_key would also improve performance once the dataset gets larger.

4. **Task 2 update strategy**

- The model for the task 2 was made fully incremental to allow for very frequent updates : it is fetching the last day of data but could also be run more frequently if performance allows it. As long as the cutoff time chosen is in line with expectations and matches across sources, it can be discussed with the stakeholders. The backfill_variable allows for full backfills when it is included in the DBT command running the model.

- The default delivery radius calculation realistically cannot be run every time an event comes in, so something in line with expectations from stakeholders is advisable (once a day, twice a day, hourly, every 15 minutes, every minute if and only if the performance end-to-end allows it). One important note is that delivery radiuses become default ones after 24 hours, so something could be implemented to update specifically the ones that are most likely to change (consistent intervals nearing 24h of constant delivery radius different from the default one) in a complex incremental logic, with the risk of overengineering the logic.

