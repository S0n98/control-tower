# Offline-safe replacement for `_PIP_ADDITIONAL_REQUIREMENTS: "trino kubernetes httpx"`
# (see CLAUDE.md gotcha #10 and dq/dq_tier_a_dag.py / dq/full_pipeline_demo_dag.py, which
# import these). _PIP_ADDITIONAL_REQUIREMENTS re-installs from PyPI on every container
# start - fine on a host with outbound internet, a hard failure in an air-gapped target.
# This bakes the same three packages in at BUILD time instead, on a machine that has
# internet access, so the resulting image needs nothing at runtime. Build this once,
# export it, import it on the air-gapped cluster exactly like control-tower's image
# (see INSTALL.md) - then remove _PIP_ADDITIONAL_REQUIREMENTS from the scheduler and
# dag-processor Deployments entirely and point them at this image instead.
FROM apache/airflow:3.2.2
USER airflow
RUN pip install --no-cache-dir trino kubernetes httpx
