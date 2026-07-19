Kubernetes objects for the RxVigilance workload namespace. Applied with                                                                                                       
`kubectl` (Phase 10 moves the applying into the deploy pipeline). The                                                                                                         
platform itself (GKE cluster, Helm releases) lives in `infra/terraform/`                                                                                                      
and is NOT managed from this directory.

| File | What |                                                                                                                                                               
  |---|---|                                                                                                                                                                     
| `namespace.yaml` | `rx-vigilance` namespace — all workload objects live here |                                                                                              
| `flink/flink-serviceaccount.yaml` | KSA `flink` with the Workload Identity annotation → GSA `rx-vigilance-sa` (GCS checkpoint access, no key files) |                       
| `flink/flink-deployment.yaml` | FlinkDeployment CR (arrives in Phase 2) |                                                                                                   

Apply order (namespace first — everything else is namespaced):

      kubectl apply -f k8s/namespace.yaml                                                                                                                                       
      kubectl apply -f k8s/flink/flink-serviceaccount.yaml 

## Kafka credentials Secret (manual — never committed)

The Flink job authenticates to Redpanda Cloud with SASL/SCRAM as the                                                                                                          
`rx-vigilance-flink` user. Those credentials exist in exactly two places:                                                                                                     
Redpanda Cloud, and `~/.redpanda-cloud.env` on the operator's machine                                                                                                         
(outside this repo). They are never written to git, Terraform state, or                                                                                                       
any manifest — this Secret is created imperatively:

      source ~/.redpanda-cloud.env                                                                                                                                              
      kubectl create secret generic kafka-credentials \                                                                                                                         
        --namespace rx-vigilance \                                                                                                                                              
        --from-literal=sasl-username=rx-vigilance-flink \                                                                                                                       
        --from-literal=sasl-password=<REDPANDA_PASSWORD> 

(In practice the password comes from `$TF_VAR_redpanda_flink_password`                                                                                                        
in the env file — shown as a placeholder here on purpose.)

Name contract (frozen; consumed by `flink-deployment.yaml` in Phase 2):                                                                                                       
Secret `kafka-credentials`, keys `sasl-username`, `sasl-password`.

Because the runtime cluster is disposable (D8), this Secret dies with                                                                                                         
every `terraform destroy` and must be recreated on every fresh cluster —                                                                                                      
`make infra-up` (#23) automates exactly that from the same env vars.

Verify without printing values:

      kubectl describe secret kafka-credentials -n rx-vigilance                                                                                                                 

## Notes

- A Secret's values are base64-encoded, not encrypted — protection is                                                                                                         
  RBAC + namespace scoping + etcd encryption at rest, not the format.
- The Workload Identity pair `[rx-vigilance/flink]` and the Secret/key                                                                                                        
  names above are name-frozen against IAM bindings and Phase 2 manifests;                                                                                                     
  renaming any of them is a coordinated change, not a cleanup.  