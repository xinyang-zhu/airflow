import importlib
import importlib.util
import logging
import sys
from logging import Logger

from sqlalchemy import Column, Text, String
from sqlalchemy.orm import Session

from airflow.configuration import conf
from airflow.models import Base
from airflow.utils.session import provide_session
from airflow.utils.timeout import timeout


class DynamicWorkflow(Base):
    """
    A table for storing dynamic workflows
    """

    __tablename__ = 'dynamic_workflow'

    dag_id = Column(String(1000), nullable=False, primary_key=True)
    source_code = Column(Text, nullable=False)

    def __init__(self, dag_id: str, source_code: str):
        self.dag_id = dag_id
        self.source_code = source_code

    def __repr__(self):
        return "<DynamicWorkflow(dag_id='%s')>" % self.dag_id

    @classmethod
    @provide_session
    def get(cls, dag_id: str, session: Session = None):
        return session.query(cls).filter(cls.dag_id == dag_id).first()

    @classmethod
    @provide_session
    def get_all(cls, session: Session = None):
        return session.query(cls).all()


def load_modules_from_db(timeout_secs=None, log: Logger = logging.getLogger()):
    """
    Load All Airflow Dynamic Workflows from DB
    """

    if timeout_secs is None:
        timeout_secs = conf.getfloat('core', 'DAGBAG_IMPORT_TIMEOUT')

    print("Eihei!")
    workflows = DynamicWorkflow.get_all()
    if not workflows or len(workflows) == 0:
        log.warning('No Dynamic Workflows found')
        return []

    loaded_mods = []
    for workflow in workflows:
        dag_id = workflow.dag_id
        mod_name = f'unusual_prefix_dynamic_workflow_{dag_id}'
        if mod_name in sys.modules:
            del sys.modules[mod_name]

        timeout_msg = f"DagBag import timeout for Dynamic Workflow: '{dag_id}' after {timeout_secs}s"
        with timeout(timeout_secs, error_message=timeout_msg):
            try:
                spec = importlib.util.spec_from_loader(mod_name, loader=None)
                new_module = importlib.util.module_from_spec(spec)
                exec(workflow.source_code, new_module.__dict__)
                loaded_mods.append(new_module)
            except Exception as e:  # pylint: disable=broad-except
                log.exception(f"Failed to import dynamic workflow: {dag_id}", exc_info=e)
        pass

    log.info(f'Loaded {len(loaded_mods)} Dynamic Workflows!')
    return loaded_mods
