@Library(value='jenkins-pipeline-library@master', changelog=false) _
pipelineRDPCWorkflowManagementScheduler(
    buildImage: "openjdk:11",
    dockerRegistry: "ghcr.io",
    dockerRepo: "icgc-argo/workflow-management-scheduler",
    gitRepo: "icgc-argo/workflow-management-scheduler",
    testCommand: "./mvnw test -ntp",
    helmRelease: "management-scheduler"
)
