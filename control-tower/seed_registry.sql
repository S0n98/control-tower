-- Seeds the pipeline table from registry/*.yaml. The Control Tower service's
-- registry loader (app/registry.py) does this on startup for real; this file
-- is the same content applied by hand for the initial bootstrap/demo.
INSERT INTO pipeline (pipeline_id, domain, tier, owner, owner_email, schedule, sla_json, components_json, outputs_json, upstream_json)
VALUES
('sparkpi_sample', 'platform', 'P3', 'team-platform', 'team-platform@company.internal', 'manual',
  '{"max_duration_minutes":15}', '[{"type":"spark","id":"sparkpi-sample","namespace":"default"}]', '[]', '[]'),
('nifi_ingest_sample', 'platform', 'P3', 'team-platform', 'team-platform@company.internal', 'continuous',
  '{"freshness_target_minutes":5}', '[{"type":"nifi","id":"nifi-0","namespace":"nifi"}]', '[]', '[]'),
('trigger_flink_job', 'platform', 'P2', 'team-platform', 'team-platform@company.internal', '*/5 * * * *',
  '{"max_duration_minutes":10}', '[{"type":"airflow","id":"trigger_flink_job","namespace":"airflow"}]', '[]', '[]'),
('sample_statemachine', 'platform', 'P2', 'team-platform', 'team-platform@company.internal', 'continuous',
  '{"freshness_target_minutes":5}', '[{"type":"flink","id":"sample-statemachine","namespace":"flink"}]', '[]', '[]'),
('kafka_to_iceberg_demo', 'platform', 'P3', 'team-platform', 'team-platform@company.internal', 'continuous',
  '{"freshness_target_minutes":5}', '[{"type":"flink","id":"kafka-to-iceberg-demo","namespace":"flink"}]',
  '[{"iceberg":"iceberg.demo.kafka_events"}]', '[{"kafka_topic":"demo-events"}]')
ON CONFLICT (pipeline_id) DO UPDATE SET
  domain = EXCLUDED.domain, tier = EXCLUDED.tier, owner = EXCLUDED.owner,
  owner_email = EXCLUDED.owner_email, schedule = EXCLUDED.schedule,
  sla_json = EXCLUDED.sla_json, components_json = EXCLUDED.components_json,
  outputs_json = EXCLUDED.outputs_json, upstream_json = EXCLUDED.upstream_json,
  updated_at = now();
