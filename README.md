# Renterd Integrity

This repository contains a small tool that runs some integrity set on a dataset managed by `renterd`. It will upload data until the dataset contains the configured amount of data, after which it will periodically delete and reupload data, as well as download files at random to then verify their integrity while at the same time making sure they're available on the Sia network.

In the (near) future this tool will be extended to automatically prune contracts, serving as a first production test for contract pruning. Since it uploads and downloads data to and from the network continuously, we can keep various statistics to detect potential performance regressions in future versions of `renterd`. Currently the tool will register an alert if it detects download issues or data corruption, this can be extended however to ping a discord bot to notify us of any production issues.

```yaml
{
  busAddress: "http://localhost:9980/api/bus",
  busPassword: "supersecret",

  workerAddress: "http://localhost:9980/api/worker",
  workerPassword: "supersecret",

  integrityCheckInterval: "1h",
  integrityCheckCyclePct: .05,

  datasetSize: 137438953472, # 128 GiB
  minFilesize: 65536, # 64KiB
  maxFilesize: 4294967296, # 4GiB

  cleanStart: false,
  workDir: "data"
}
```
