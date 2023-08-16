# Copyright (c) 2023, Ross Helenius
# All rights reserved.

# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. 

import datetime
import pytz
import tableauserverclient as TSC
import os
import configparser

def calculate_duration_from_utc(start_datetime_utc):
    start_datetime_utc = datetime.datetime.strptime(start_datetime_utc, "%Y-%m-%d %H:%M:%S%z")
    current_datetime_utc = datetime.datetime.now(pytz.utc)
    duration = current_datetime_utc - start_datetime_utc
    return duration

if __name__ == "__main__":
    try: 
        #read credentials from the config file
        config = configparser.ConfigParser()
        config.read('config.ini')
        name = config.get('tableau', 'name')
        pat = config.get('tableau', 'pat')
        site = config.get('tableau', 'site')
        timeout = config.get('tableau', 'minutes')
        cloud_server = config.get('tableau', 'server')

        tableau_auth = TSC.PersonalAccessTokenAuth(name, pat, site_id=site)
        server = TSC.Server(cloud_server, use_server_version=True)    
        server.auth.sign_in(tableau_auth)

        with server.auth.sign_in(tableau_auth):
            req = TSC.RequestOptions()
            #get jobs that are in progress with a run time greater than 0
            req.filter.add(TSC.Filter("progress", TSC.RequestOptions.Operator.GreaterThanOrEqual, 0))
            req.filter.add(TSC.Filter("status", TSC.RequestOptions.Operator.Equals, "InProgress"))
            target_jobs = (job for job in TSC.Pager(server.jobs, request_opts=req) )

            in_progress_entries = [job for job in target_jobs if job.type == 'run_flow' and job.status == 'InProgress']

            #find the jobs that are in progress and parse their attributes
            if in_progress_entries:
                for entry in in_progress_entries:
                    print("Action:", entry.type)
                    print("Run ID:", entry.id)
                    print("Status:", entry.status)
                    print("Start Time:", entry.started_at)

                    if entry.status == "InProgress":
                        duration = calculate_duration_from_utc(str(entry.started_at))
                        print("Duration:", duration)
                        if duration > datetime.timedelta(minutes=int(timeout)):
                            # Cancel the job with the given run ID, if greater than our cogured timeout
                            try:
                                server.jobs.cancel(entry.id)
                                status = "Cancelled"
                                cancelled = datetime.datetime.now(pytz.utc)

                                # Append the job information to the log file
                                with open('jobs.txt', 'a') as f:
                                    f.write(f"{entry.type}, {entry.id}, {status}, {entry.started_at}, {cancelled}, {duration}\n")
                            except TSC.ServerResponseError as error:
                                print(f"Error cancelling job {entry.id}: {error}")
                        else: 
                            print("Job has not exceeded timeout.")
            else:
                print("No entries with status 'InProgress' found.")

    except Exception as e:
        print(f"An error occurred: {e}")
