from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor

import os
import json

from .jobs import bird_feeders_table_job


@sensor(job=bird_feeders_table_job)
def bird_observation(context: SensorEvaluationContext):
    PATH_TO_BIRD_FILES = os.path.join(os.path.dirname(__file__), "../data/bird_data")
    previous_state = json.loads(context.cursor) if context.cursor else {}
    current_state = {}

    files_to_process = []
    for filename in os.listdir(PATH_TO_BIRD_FILES):
        file_path = os.path.join(PATH_TO_BIRD_FILES, filename)
        if (
            filename.startswith("PFW_all")
            and filename.endswith(".csv")
            and os.path.isfile(file_path)
        ):
            context.log.info(f"Found bird observation file {filename}")
            last_modified = os.path.getmtime(file_path)

            current_state[filename] = last_modified

            # if the file is new or has been modified since the last run, add it to the queue
            if (
                filename not in previous_state
                or previous_state[filename] != last_modified
            ):
                files_to_process.append(
                    RunRequest(
                        run_key=f"bird_observation_{filename}_{last_modified}",
                        run_config={
                            "ops": {
                                "bird_feeders_table": {
                                    "config": {
                                        "filename": filename,
                                    }
                                }
                            }
                        },
                    )
                )

    return SensorResult(run_requests=files_to_process, cursor=json.dumps(current_state))
