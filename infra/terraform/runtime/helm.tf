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

