from airflow.models import DagRun
from airflow.utils.state import State
from airflow.utils.session import provide_session


@provide_session
def get_most_recent_init_dag_success(execution_date, session=None, **context):
    """
    Returns the execution date of the most recent successful 'init' DAGRun.
    Used as 'execution_date_fn' for ExternalTaskSensor.
    """
    dag_run = (
        session.query(DagRun)
        .filter(DagRun.dag_id == "init", DagRun.state == State.SUCCESS)
        .order_by(DagRun.execution_date.desc())
        .first()
    )
    
    if dag_run:
        return dag_run.execution_date
    else:
        return execution_date