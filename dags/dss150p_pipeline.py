from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

PROJECT = '/opt/airflow/project'


def failure_callback(context):
    """Write a concise failure record to the task log.

    Airflow already persists this into the task's log file (visible in the
    Grid/Graph view), which is the run evidence required for Task E.
    """
    ti = context['task_instance']
    print(
        f"TASK FAILED: dag_id={ti.dag_id} task_id={ti.task_id} "
        f"run_id={context['run_id']} try_number={ti.try_number} "
        f"execution_date={context.get('logical_date')} "
        f"exception={context.get('exception')}"
    )


def choose_load_branch(**context):
    """Route to the full-load or partition-load task based on params.run_mode."""
    run_mode = context['params']['run_mode']
    return 'load_full' if run_mode == 'full' else 'load_partition'


DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=15),
    'on_failure_callback': failure_callback,
}

with DAG(
    dag_id='dss150p_sales_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule='0 2 * * *',
    # Runs on a fixed daily cadence against a slowly-changing source snapshot
    # rather than an event-driven feed, so a simple daily cron at a low-traffic
    # hour (02:00) is appropriate -- no sub-daily freshness requirement exists.
    catchup=False,
    # The pipeline recomputes staging/curated from the current source
    # snapshot on every run rather than incrementally consuming a specific
    # historical data interval, so backfilling missed calendar days would
    # only ever reprocess the same current snapshot multiple times. That
    # gives no analytical benefit and risks needless load/duplicate work,
    # so catchup is disabled (see Week 7 backfill-reasoning notes in
    # templates/run_evidence_template.md).
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition']),
        'year': Param(2026, type='integer'),
        'month': Param(1, type='integer', minimum=1, maximum=12),
    },
    tags=['DSS150P'],
) as dag:
    extract = BashOperator(
        task_id='extract',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli extract',
    )
    transform = BashOperator(
        task_id='transform',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli transform',
    )
    branch = BranchPythonOperator(
        task_id='choose_load_branch',
        python_callable=choose_load_branch,
    )
    load_full = BashOperator(
        task_id='load_full',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli load',
    )
    load_partition = BashOperator(
        task_id='load_partition',
        bash_command=(
            f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" '
            'python -m src.cli benchmark --repeats 1 && '
            f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" '
            'python -m src.cli load-partition '
            '--year {{ params.year }} --month {{ params.month }}'
        ),
    )
    join = EmptyOperator(task_id='load_done', trigger_rule='none_failed_min_one_success')
    validate = BashOperator(
        task_id='validate',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli validate',
    )

    # run_mode=full   -> extract -> transform -> load_full -> validate
    # run_mode=partition -> extract -> transform -> load_partition (also
    #   materializes data/partitioned/ via `benchmark --repeats 1`) -> validate
    # The branch task skips whichever load path was not selected; `join`
    # uses trigger_rule=none_failed_min_one_success so the skip does not
    # block validate.
    extract >> transform >> branch >> [load_full, load_partition] >> join >> validate
