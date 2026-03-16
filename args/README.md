# Args Files

## What lives here

- checked-in example args files
- generated local preset args files written by `scripts/setup_uv.sh`

## Checked-in examples

- `autonomous_vm.example.yaml`
- `spiky_autonomous_vm.example.yaml`
- `fio_distributed.example.yaml`
- `mixed_pressure.example.yaml`
- `net_many_to_one.example.yaml`
- `net_ring.example.yaml`
- `capacity_sweep.example.yaml`

These are reference shapes, not the preferred Sunbeam operator path.

## Generated files

Typical generated files:

- `args/smoke.yaml`
- `args/steady.yaml`
- `args/spiky.yaml`
- `args/failure-storm.yaml`
- `args/quota-edge.yaml`
- `args/tenant-churn.yaml`
- `args/fio-distributed.yaml`
- `args/mixed-pressure.yaml`
- `args/net-many-to-one.yaml`
- `args/net-many-to-one-http.yaml`
- `args/net-ring.yaml`
- `args/stress-ng.yaml`

These are produced by:

```bash
./scripts/setup_uv.sh /path/to/clouds.yaml <preset>
```

## Guidance

- prefer generated preset args for Sunbeam
- generated preset args are annotated with the preset role, required
	services and images, and the first knobs to tune
- use the checked-in examples when you need to understand the schema shape
- treat local generated args as environment-specific, not as stable reference docs
- treat `smoke` as the smallest connectivity check and `steady` as the first
	real autonomous VM baseline
