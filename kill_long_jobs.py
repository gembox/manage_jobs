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

def parse_log_data(log_data):
    log_entries = log_data.strip().split('\n')
    parsed_data = []

    for entry in log_entries:
        fields = entry.split(', ')
        action = fields[0]
        run_id = fields[2]
        status = fields[3]
        start_time = fields[4]
        end_time = fields[5]

        parsed_entry = {
            "action": action,
            "run_id": run_id,
            "status": status,
            "start_time": start_time,
            "end_time": end_time
        }

        parsed_data.append(parsed_entry)

    return parsed_data

if __name__ == "__main__":
    log_file_path = "log_file.txt"

    #read credentials from the config file
    #setup these values in the config.ini file template provided on the project
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

        with open('jobs.txt', 'w') as f:
            for job in target_jobs: 
                if job.type == 'run_flow':
                    f.write(f"{job.type}, {job.name}, {job.id}, {job.status}, {job.started_at}, {job.ended_at}\n")

        try:
            with open('jobs.txt', "r") as log_file:
                log_data = log_file.read()

            parsed_data = parse_log_data(log_data)

            in_progress_entries = [entry for entry in parsed_data if entry["status"] == "InProgress"]

            #find the jobs that are in progress
            if in_progress_entries:
                for entry in in_progress_entries:
                    print("Action:", entry["action"])
                    print("Run ID:", entry["run_id"])
                    print("Status:", entry["status"])
                    print("Start Time:", entry["start_time"])
                    
                    if entry["end_time"] == "None":
                        print("End Time: Still In Progress")
                    else:
                        print("End Time:", entry["end_time"])

                    if entry["status"] == "InProgress":
                        duration = calculate_duration_from_utc(entry["start_time"])
                        print("Duration:", duration)
                        if duration > datetime.timedelta(minutes=timeout):
                            # Cancel the job with the given run ID, if greater than our cogured timeout
                            server.jobs.cancel(entry["run_id"])
                            entry["status"] = "Cancelled" #update the status of the job in the parsed data
                            entry["end_time"] = cancellation_time.strftime("%Y-%m-%d %H:%M:%S%z")  # Update end time to the time of cancellation
                
                            #write the job back to the parsed file
                            with open('jobs.txt', 'w') as f:
                                for entry in parsed_data:
                                    f.write(f"{entry['action']}, {entry['run_id']}, {entry['status']}, {entry['start_time']}, {entry['end_time']}, {duration}\n")

                        else:
                            print("No entries with status 'InProgress' and greater than duration found.")

        except FileNotFoundError:
            print(f"Log file not found at {log_file_path}")
