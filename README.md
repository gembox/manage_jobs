# Cancel Long Running Flow Jobs - Tableau Server
In order to free up resources for Tableau flow jobs that run too long, we can use this script to determine the duration that we want to allow jobs to run for before we cancel them. 
The script will look at the flow jobs, determine how long they have been running and cancel them if over the threshold set in the config file.

The config file template should be populated with a few pieces of information in order to connect and determine thresholds. You need to setup a personal access token on a user in order to authenticate to Cloud.

**site** - the name of your Tableau Cloud site\
**name** - the name of your Personal Access Token\
**pat** - the secret assigned to your token\
**server** - the base url of your tableau server region\
**minutes** - the duration in minutes that you would like to cancel jobs if they run past\
