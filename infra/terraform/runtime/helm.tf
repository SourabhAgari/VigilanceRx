resource "helm_release" "cert_manager" {
  name             = "cert-manager"
  repository       = "https://charts.jetstack.io"
  chart            = "cert-manager"
  version          = "v1.21.0" # pinned: same apply = same software
  namespace        = "cert-manager"
  create_namespace = true

  set = [
    {
      name  = "crds.enabled"
      value = "true"
    }
  ]

  # Chart ships no requests: all three pods land as BestEffort, first evicted
  # under node pressure. Actual usage is negligible; this is about eviction
  # order, not capacity (#115, D55).
  values = [<<-EOT
  resources:
    requests: { cpu: 20m, memory: 64Mi }
    limits:   { memory: 128Mi }

  webhook:
    resources:
      requests: { cpu: 20m, memory: 64Mi }
      limits:   { memory: 128Mi }

  cainjector:
    resources:
      requests: { cpu: 20m, memory: 128Mi }
      limits:   { memory: 256Mi }
EOT
  ]
}

resource "helm_release" "flink_operator" {
  name             = "flink-kubernetes-operator"
  repository       = "https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/"
  chart            = "flink-kubernetes-operator"
  version          = "1.15.0"
  namespace        = "flink-system"
  create_namespace = true

  # No attribute ties these releases together, but the operator's admission
  # webhook needs cert-manager ready to issue its TLS cert (§10 chain).
  depends_on = [helm_release.cert_manager]
  # The operator is what keeps the job running and handles upgrades. It was
  # BestEffort, so it was first in line to be evicted (#115). Measured 385Mi -
  # it is a JVM, so memory dominates.
  values = [<<-EOT
  operatorPod:
    resources:
      requests: { cpu: 100m, memory: 512Mi }
      limits:   { memory: 1Gi }

  webhook:
    resources:
      requests: { cpu: 50m, memory: 128Mi }
      limits:   { memory: 256Mi }
EOT
  ]
}

resource "helm_release" "kube_prometheus_stack" {
  name             = "kube-prometheus-stack"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  version          = "87.17.0"
  namespace        = "monitoring"
  create_namespace = true

  # No depends_on: this chart self-manages its webhook certs and shares no
  # real dependency with cert-manager or the Flink operator.
  # Six BestEffort pods, including the two that would tell us something is
  # wrong. Values below are measured usage (#115) with headroom. grafana,
  # kube-state-metrics and prometheus-node-exporter are subcharts - their keys
  # are not in this chart's own values file but pass through normally.
  values = [<<-EOT
  prometheusOperator:
    resources:
      requests: { cpu: 20m, memory: 64Mi }
      limits:   { memory: 128Mi }

  prometheus:
    prometheusSpec:
      resources:
        requests: { cpu: 100m, memory: 512Mi }
        limits:   { memory: 1Gi }

  alertmanager:
    alertmanagerSpec:
      resources:
        requests: { cpu: 20m, memory: 64Mi }
        limits:   { memory: 128Mi }

  grafana:
    resources:
      requests: { cpu: 50m, memory: 384Mi }
      limits:   { memory: 512Mi }

    # Downloaded at pod start, not baked into the image — so the pod needs
    # egress to grafana.com and takes longer to become Ready. A pod that
    # cannot reach the plugin repository fails without saying "plugin" (#181).
    plugins:
      - googlecloud-logging-datasource

    serviceAccount:
        # Not pinned by name, unlike external-secrets below: the Workload
        # Identity binding in platform/grafana.tf names the chart's own generated
        # account, monitoring/kube-prometheus-stack-grafana, which was confirmed
        # against the cluster before the binding was written.
        annotations:
          iam.gke.io/gcp-service-account: rx-vigilance-grafana@vigilancerx-502702.iam.gserviceaccount.com
    # Provisioned here rather than as a labelled ConfigMap: the datasources
    # sidecar has no NAMESPACE set, so it only watches monitoring — a ConfigMap
    # under k8s/flink (namespace rx-vigilance) would be invisible to it. The uid
    # is pinned because the #182 log dashboards reference it.
    additionalDataSources:
      - name: GCP Cloud Logging
        type: googlecloud-logging-datasource
        uid: gcp-cloud-logging
        access: proxy
        isDefault: false
        editable: false
        jsonData:
          authenticationType: gce

  kube-state-metrics:
    resources:
      requests: { cpu: 20m, memory: 64Mi }
      limits:   { memory: 128Mi }

  prometheus-node-exporter:
    resources:
      requests: { cpu: 20m, memory: 32Mi }
      limits:   { memory: 64Mi }
EOT
  ]
}

resource "helm_release" "argocd" {
  name             = "argocd"
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-cd"
  version          = "10.3.0" # pinned: same apply = same software (Argo CD v3.5.0)
  namespace        = "argocd"
  create_namespace = true

  # No depends_on: Argo CD issues its own TLS internally and does not use
  # cert-manager, unlike the Flink operator's admission webhook (§10 chain).
  # Chart defaults are non-HA, which is what D54 chose — redis-ha and multiple
  # controller replicas are theatre on a two-node spot pool.
  # The chart ships NO resource requests, so all 7 pods land as BestEffort:
  # invisible to the scheduler, and first to be evicted under node pressure —
  # the worst QoS class for the component that decides what gets deployed, on
  # a pool whose nodes are already preemptible.
  values = [<<-EOT
      # Not used here, so not run. Re-enable deliberately if the need appears.
      dex:
        enabled: false          # SSO only; there is no identity provider
      notifications:
        enabled: false          # Slack/email on sync events; not wired up

      # CPU requests but no CPU limits on purpose: a CPU limit throttles the
      # controller even when the node is idle, which shows up as slow syncs.
      # Memory limits are set, because memory is not compressible.
      controller:
        resources:
          requests: { cpu: 250m, memory: 512Mi }
          limits:   { memory: 1Gi }
      repoServer:
        resources:
          requests: { cpu: 100m, memory: 256Mi }
          limits:   { memory: 512Mi }
      server:
        resources:
          requests: { cpu: 100m, memory: 128Mi }
          limits:   { memory: 256Mi }
      redis:
        resources:
          requests: { cpu: 100m, memory: 128Mi }
          limits:   { memory: 256Mi }
      # Chart 10.3.0 has no applicationSet.enabled key — the controller ships
      # unconditionally, and Helm silently ignored the attempt to disable it.
      # It sits idle (no ApplicationSets defined) but must not be BestEffort.
      applicationSet:
        resources:
          requests: { cpu: 100m, memory: 128Mi }
          limits:  { memory: 256Mi }
    EOT
  ]
}

resource "helm_release" "external_secrets" {
  name             = "external-secrets"
  repository       = "https://charts.external-secrets.io"
  chart            = "external-secrets"
  version          = "2.9.0" # pinned: same apply = same software
  namespace        = "external-secrets"
  create_namespace = true

  # No depends_on cert-manager: this chart runs its own cert-controller for the
  # webhook, unlike the Flink operator (§10 chain).
  values = [<<-EOT
    # CRDs are installed by `make infra-up`, not by this release. This chart
    # ships them under templates/ rather than the reserved crds/ folder, so
    # Helm treats them as release resources and deletes them on any rollback
    # or uninstall — which is what stripped ExternalSecret in #157. Owning
    # them outside the release makes that impossible.
    installCRDs: false
    serviceAccount:
      # Pinned, not left to the chart's generated name. The Workload Identity
      # binding in platform/secrets.tf names the
      # [external-secrets/external-secrets] literally, and a generated name
      # that drifts would break impersonation with a confusing 403.
      name: external-secrets
      annotations:
        iam.gke.io/gcp-service-account: rx-vigilance-eso@vigilancerx-502702.iam.gserviceaccount.com

    # Requests from the start, per #115/D61 - this chart ships none either.
    # CPU requests without CPU limits; memory with both.
    resources:
      requests: { cpu: 50m, memory: 128Mi }
      limits:   { memory: 256Mi }

    webhook:
      resources:
        requests: { cpu: 20m, memory: 64Mi }
        limits:   { memory: 128Mi }

    certController:
      resources:
        requests: { cpu: 20m, memory: 64Mi }
        limits:   { memory: 128Mi }
  EOT
  ]
}

