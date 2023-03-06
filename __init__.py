# The monitor jobs module contains all to monitor Dnd_pointatabricks jobs with ADO
# Documentacion link: https://inspirato.atlassian.net/l/cp/Bh2QiC0A

import logging
import os
import time
from typing import Any, Dict
import requests
import azure.functions as func
from fastapi import FastAPI


# ------------------ API ------------------#
app = FastAPI(debug=True)

FORMAT = "%(levelname)s:%(message)s"
logging.basicConfig(format=FORMAT, level=logging.DEBUG)

@app.get("/execute_handle_warning_state/{build_id}")
async def execute_handle_warning_state(build_id):
    path_to_file = get_path_to_file(build_id)
    state = handle_warning_state(path_to_file)
    return {"state":state}


@app.post("/execute_monitor_jobs")
async def execute_monitor_jobs(req_body: dict):
    build_id = req_body.get('build_id')
    run_ids = req_body.get('run_ids')
    project = req_body.get('project')
    organization = req_body.get('organization')
    stage_ref_name = req_body.get('stage_ref_name')
    ado_token = req_body.get('ado_token')
    databricks_base_uri = req_body.get('databricks_base_uri')
    databricks_token = req_body.get('databricks_token')

    logging.info("build_ids " + build_id)
    logging.info("run_ids: " + run_ids)
    logging.info("project: " + project)
    logging.info("organization: " + organization)
    logging.info("stage_ref_name: " + stage_ref_name)
    logging.info("ado_token: " + ado_token)
    logging.info("databricks_base_uri: " + databricks_base_uri)
    logging.info("databricks_token : " + databricks_token)

    translate_states = {
        "RUNNING": "",
        "INTERNAL_ERROR": "cancel",
        "TERMINATED": "retry",
        "SKIPPED": "retry",
    }
    path_to_file = get_path_to_file(build_id)
    end_point = (
        f"https://dev.azure.com/{organization}/"
        f"{project}/_apis/build/builds/{build_id}/"
        f"stages/{stage_ref_name}?api-version=7.0"
    )
    is_build_state = validate_build_state(path_to_file)
    logging.info(f"is_build_state: {is_build_state}")
    if is_build_state:
        run_state = wait_runs_completion(
            run_ids, databricks_base_uri, databricks_token
        )
        state = translate_states[run_state]
        update_build_state(path_to_file, state)
        wait_monitor_stage_completition(
            ado_token, end_point, state
        )


async def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    """Each request is redirected to the ASGI handler."""
    return await func.AsgiMiddleware(app).handle_async(req, context)


# ------------------ FILE SYSTEM ------------------#
def get_path_to_file(build_id: str) -> str:
    path_to_folder = os.path.dirname("/tmp/")
    file_name = f"build_id_{build_id}.txt"
    file_path = os.path.join(path_to_folder, file_name)
    return file_path


def read_build_state(path_to_file: str) -> None:
    with open(path_to_file, "r", encoding="utf8") as file:
        state = file.read()
    return state


def update_build_state(path_to_file, state: str) -> None:
    with open(path_to_file, "w", encoding="utf8") as file:
        file.write(state)
    file.close()


def handle_warning_state(path_to_file: str) -> str:
    state = "WARNING"
    file_exists = os.path.isfile(path_to_file)
    if file_exists:
        state = read_build_state(path_to_file)
    else:
        update_build_state(path_to_file, state)
    if state != "WARNING":
        os.remove(path_to_file)
    return state


def validate_build_state(path_to_file: str) -> bool:
    validation = False
    file_exists = file_exists = os.path.isfile(path_to_file)
    if not file_exists:
        return validation
    state = read_build_state(path_to_file)
    if state == "WARNING":
        validation = True
    logging.info("Validate build state: " + str(validation))
    return validation


# ------------------ DATABRICKS ------------------#
def get_run(
    databricks_base_uri: str, databricks_token: str, run_id: int
) -> Dict:
    headers = {
        "content-type": "application/json",
        "Authorization": f"Bearer {databricks_token}",
    }
    with requests.Session() as session_request:
        response = session_request.get(
            f"{databricks_base_uri}/api/2.1/jobs/runs/get",
            json={"run_id": run_id},
            headers=headers,
        )
        response.raise_for_status()
    return response.json()


def get_runs_status_info(
    databricks_base_uri: str, databricks_token: str, run_ids: list
) -> Dict[Any, str]:
    runs_status_info = {}
    for run_id in run_ids:
        run_info = get_run(databricks_base_uri, databricks_token, run_id)
        runs_status_info[run_id] = {
            "job_id": run_info["job_id"],
            "run_status": run_info["state"]["life_cycle_state"],
        }
    return runs_status_info


def get_runs_status_report(runs_status_info: Dict[Any, Any]) -> Dict[str, int]:
    runs_status_report = {
        "RUNNING": 0,
        "SKIPPED": 0,
        "INTERNAL_ERROR": 0,
        "TERMINATED": 0,
    }
    for run_id, run_info in runs_status_info.items():
        run_condition = run_info["run_status"]
        if run_condition in runs_status_report.keys():
            runs_status_report.update(
                {run_condition: runs_status_report[run_condition] + 1}
            )
            job_id = run_info["job_id"]
            log_message = (
                "STATUS "
                + str(run_condition)
                + " ,JOB ID "
                + str(job_id)
                + " ,RUN ID "
                + str(run_id)
            )
            logging.info(log_message)
    return runs_status_report


def validate_runs_state(runs_status_report: Dict[Any, int]) -> str:
    if runs_status_report["RUNNING"] > 0:
        run_state = "RUNNING"
    elif runs_status_report["INTERNAL_ERROR"] > 0:
        run_state = "INTERNAL_ERROR"
    elif runs_status_report["TERMINATED"] > 0:
        run_state = "TERMINATED"
    else:
        run_state = "SKIPPED"
    logging.info("Validation state: " + run_state)
    return run_state


def monitor_runs(
    run_ids: str,
    databricks_base_uri: str,
    databricks_token: str,
) -> str:
    run_ids = run_ids.split(",")
    run_ids_list = [int(run_id) for run_id in run_ids]
    if run_ids_list:
        runs_status_info = get_runs_status_info(
            databricks_base_uri, databricks_token, run_ids
        )
        logging.info(f"runs_status_info: {str(runs_status_info)}")
        runs_status_report = get_runs_status_report(runs_status_info)
        logging.info(f"runs_status_report: {str(runs_status_report)}")
        state = validate_runs_state(runs_status_report)
    else:
        state = "SUCCESS"
        logging.info("There are not jobs to monitor")
    return state


def wait_runs_completion(
    run_ids: str,
    databricks_base_uri: str,
    databricks_token: str,
):
    polling_interval = 60
    timeout = 3600
    start_time = time.time()
    elapsed_time = time.time() - start_time
    while timeout > elapsed_time:
        run_state = monitor_runs(
            run_ids,
            databricks_base_uri,
            databricks_token,
        )
        if run_state != "RUNNING":
            break
        time.sleep(polling_interval)
        elapsed_time = time.time() - start_time
    return run_state


# ------------------ ADO ------------------#
def update_monitor_stage_state(
    ado_token: str, end_point: str, state: str
) -> int:
    head = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {ado_token}",
    }
    payload = {"forceRetryAllJobs": True, "state": state}
    response = requests.patch(end_point, headers=head, json=payload)
    logging.info(
        "Update monitor stage state status code: " + str(response.status_code)
    )
    status_code = response.status_code
    logging.info("Update monitor stage state status code: " + str(status_code))
    return status_code


def wait_monitor_stage_completition(ado_token, end_point, state) -> int:
    polling_interval = (60,)
    timeout = 3600
    start_time = time.time()
    elapsed_time = time.time() - start_time
    while timeout > elapsed_time:
        status_code = update_monitor_stage_state(ado_token, end_point, state)
        if status_code != 409:
            break
        time.sleep(polling_interval)
        elapsed_time = time.time() - start_time
    return status_code