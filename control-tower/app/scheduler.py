import logging

from apscheduler.schedulers.background import BackgroundScheduler
from kubernetes import config as k8s_config

from . import metrics, sla_engine
from .collectors import airflow_collector, flink_collector, nifi_collector, spark_collector
from .collectors import registry_reconciler
from .registry import sync_registry_to_db

log = logging.getLogger("control_tower.scheduler")

try:
    k8s_config.load_incluster_config()
except k8s_config.ConfigException:
    log.warning("no in-cluster kubeconfig found; spark/flink collectors will fail")


def poll_cycle():
    try:
        sync_registry_to_db()
        observed = []
        observed += spark_collector.collect()
        observed += flink_collector.collect()
        observed += nifi_collector.collect()
        airflow_collector.collect()
        registry_reconciler.reconcile(observed)
        sla_engine.evaluate()
        metrics.refresh()
    except Exception:
        log.exception("poll cycle failed")


def start() -> BackgroundScheduler:
    sched = BackgroundScheduler()
    sched.add_job(poll_cycle, "interval", seconds=30, id="poll_cycle",
                  max_instances=1, coalesce=True, next_run_time=None)
    sched.start()
    # run once immediately, in-thread, so /metrics has data right after startup
    poll_cycle()
    from datetime import datetime, timedelta
    sched.modify_job("poll_cycle", next_run_time=datetime.now() + timedelta(seconds=30))
    return sched
