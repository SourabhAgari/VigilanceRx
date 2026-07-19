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
  version          =  "87.17.0"
  namespace        = "monitoring"
  create_namespace = true

  # No depends_on: this chart self-manages its webhook certs and shares no
  # real dependency with cert-manager or the Flink operator.
}