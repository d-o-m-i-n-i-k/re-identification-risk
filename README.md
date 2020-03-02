# Quantifying the Re-identification Risk of Event Logs for Process Mining

The re-identification risk is quantified with measures for the individual uniqueness in event logs. 

Based on the approach presented in:
S. Nuñez von Voigt, S.A. Fahrenkrog-Petersen, D. Janssen, A. Koschmider, F. Tschorsch, F. Mannhardt, O. Landsiedel and M. Weidlich. Quantifying the Re-identification Risk\\of Event Logs for Process Mining. In ...


## Installation
### Clone
Clone this repo to your local machine using ...
### Requirements

## Usage
### Step 1 Preparation of the data
Summarize all event data that occurs for the corresponding case. After conversion, each row in this data set belongs to one case. 
- Specify the path of the event log (csv)
- Specify the unique identifier and the list of attributes to consider
...

### Step 2 Calculate unicity 
Unicity is the number of cases that are uniquely identifiable by the set of case attributes or the set of event attributes.
In *unicity_activities.py* specify:
- the path for the event log (output of Step 1).
- the columnnames for the unique identifier, activities, timestamp, event and case attributes.
- the projection that should be considered.
Projection refer to a subset of attributes in the event log.
It is possible to specify the number of points as an absolute number or as a relative frequency.
The algorithm outputs the ratio of unique cases to the total number of cases.
