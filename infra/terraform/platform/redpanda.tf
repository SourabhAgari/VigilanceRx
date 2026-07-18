resource "redpanda_resource_group" "rx_vigilance" {
  name = "rx-vigilance"
}

resource "redpanda_serverless_cluster" "main" {
  name              = "rx-vigilance"
  resource_group_id = redpanda_resource_group.rx_vigilance.id
  serverless_region = var.redpanda_serverless_region

  # allow_deletion defaults to false — platform stack is never destroyed (D8);
  # a `terraform destroy` here must fail loudly, not take the topics with it.
}

locals {
  # Mirror of Phase 0 local bootstrap (D6): 3 partitions on the event
  # stream, 1 elsewhere. Ref topics are compacted: broadcast state is
  # rebuilt from the full topic on every job start, so records must
  # never age out (proposed D12).
  topics = {
    "rx-fill-events"      = { partitions = 3, config = {} }
    "ndc-drug-class-ref"  = { partitions = 1, config = { "cleanup.policy" = "compact" } }
    "alert-lead-time-ref" = { partitions = 1, config = { "cleanup.policy" = "compact" } }
    "gap-risk-alerts"     = { partitions = 1, config = {} }
    "lapsed-alerts"       = { partitions = 1, config = {} }
    "pdc-snapshots"       = { partitions = 1, config = {} }
    "dead-letter"         = { partitions = 1, config = {} }
  }
}


resource "redpanda_topic" "topics" {
  for_each = local.topics

  name = each.key
  partition_count = each.value.partitions
  cluster_api_url = redpanda_serverless_cluster.main.cluster_api_url
  configuration = each.value.config

  # replication_factor omitted: serverless clusters own replication.
  # allow_deletion stays false (default): topics are platform-persistent (D8).
}

resource "redpanda_user" "flink" {
  name                = "rx-vigilance-flink"
  password_wo         = var.redpanda_flink_password
  password_wo_version = 1 # bump to rotate the password
  mechanism           = "scram-sha-256"
  cluster_api_url     = redpanda_serverless_cluster.main.cluster_api_url
}

locals {
  # Least privilege: READ on sources, WRITE on sinks, READ on the consumer
  # group. READ/WRITE imply DESCRIBE, so no explicit DESCRIBE entries.
  flink_acls = {
    "read-rx-fill-events"       = { type = "TOPIC", name = "rx-fill-events", op = "READ", pattern = "LITERAL" }
    "read-ndc-drug-class-ref"   = { type = "TOPIC", name = "ndc-drug-class-ref", op = "READ", pattern = "LITERAL" }
    "read-alert-lead-time-ref"  = { type = "TOPIC", name = "alert-lead-time-ref", op = "READ", pattern = "LITERAL" }
    "write-gap-risk-alerts"     = { type = "TOPIC", name = "gap-risk-alerts", op = "WRITE", pattern = "LITERAL" }
    "write-lapsed-alerts"       = { type = "TOPIC", name = "lapsed-alerts", op = "WRITE", pattern = "LITERAL" }
    "write-pdc-snapshots"       = { type = "TOPIC", name = "pdc-snapshots", op = "WRITE", pattern = "LITERAL" }
    "write-dead-letter"         = { type = "TOPIC", name = "dead-letter", op = "WRITE", pattern = "LITERAL" }
    "read-consumer-group"       = { type = "GROUP", name = "rx-vigilance", op = "READ", pattern = "PREFIXED" }
  }
}

resource "redpanda_acl" "flink" {
  for_each = local.flink_acls

  resource_type         = each.value.type
  resource_name         = each.value.name
  resource_pattern_type = each.value.pattern
  principal             = "User:${redpanda_user.flink.name}"
  host                  = "*"
  operation             = each.value.op
  permission_type       = "ALLOW"
  cluster_api_url       = redpanda_serverless_cluster.main.cluster_api_url
}