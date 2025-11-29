
### Assignment Details

Upon analyzing the dataset, I observed that the available records only span October 3rd and 4th. Due to this limited date range, I configured the time grain to 10 minutes in order to achieve a clearer visualization of ticket trends. on the line chart.

Additionally, both DBT and Airflow workflows have been integrated into this repository. 
To improve monitoring and error handling capability, we can implement DBT's built-in Slack notification plugin, which triggers alerts in case any job fails.

Since the dataset covers only two days, including a week-level filter on the dashboard therefore has not been added.

### Insights from datasets. 

- Resolution Time Tickets : in Network, Software, and Finance categories took more than 80 hours on average to resolve.

- Ticket Volume Analysis: The morning hours see the highest number of incoming tickets.

- Resolution Trends by Group: Currently, the percentage of tickets resolved by individual team members is almost the same. To get better insights, we can:

Segment tickets by priority (High, Medium, Low)

Calculate the resolved tickets per person within each priority level

This will help us see how high and medium priority tickets are assigned across team members and understand workload distribution more clearly.


