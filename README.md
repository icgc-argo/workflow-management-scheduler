# Workflow Management Scheduler

## What is it?
Middleware for icgcargo/workflow-management used to initialise workflows with scheduled directories (if needed).

## How does it work?
This middleware is essentially a greedy scheduluer that treats each available directory as a knapsack which it is filling up with workflows. Each directory has a `maxCostPerDir` and each workflow configured has a `cost`. This allows for various combinations of workflows running on a single directories while making sure the `maxCostPerDir` is never exceed. There is also a `maxTotalRun` per workflow to make sure each workflow type has a fair share of directories.

The scheduler logic only applies for configured workflows repos and workflow requests that make use of the `workDirTemplate` tag. This tag is needed for scheduler to know where to inject the directory values in the run request.

## Configuration
Scheduler configuration looks like this:
```
scheduler:
  workDirTemplate: "<SCHEDULED_DIR>"
  maxCostPerDir: 3
  dirValues:
    - "nfs-local/nfs-1"
    - "nfs-local/nfs-2"
  workflows:
    - repository: "argo/hello"
      name: "hello"
      maxTotalRuns: 2
      cost: 2
    - repository: "argo/sanger-wgs"
      name: "sanger-wgs"
      maxTotalRuns: 4
      cost: 1
```

- template string is used to find and replace with the scheduled dir
- array of workflows wach configured with a repository, name, cost, and maxTotalRuns
- when making decisions, scheduler will not exceed the maxTotalRuns per workflow and maxCostPerDir
