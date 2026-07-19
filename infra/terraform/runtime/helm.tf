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