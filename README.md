# teams-league-java-standard-beam

This video present a real world use case developed with Apache Beam Java and launched with the serverless Dataflow runner in Google Cloud Platform.

The job read a Json file from Cloud Storage, applies some transformations and write the result to a BigQuery table.

The link to the video that explains this use case.
- English : https://youtu.be/kPPkERBjsSs
- French : https://youtu.be/lvFyRT-gRHA

## Run job with Dataflow runner :

### Batch

```
mvn compile exec:java \
  -Dexec.mainClass=fr.groupbees.application.TeamLeagueApp \
  -Dexec.args=" \
  --project=gb-poc-373711 \
  --runner=DataflowRunner \
  --jobName=team-league-java-job-$(date +'%Y-%m-%d-%H-%M-%S') \
  --inputJsonFile=gs://mazlum_dev/team_league/input/json/input_teams_stats_raw.json \
  --region=europe-west1 \
  --streaming=false \
  --zone=europe-west1-d \
  --tempLocation=gs://mazlum_dev/dataflow/temp \
  --gcpTempLocation=gs://mazlum_dev/dataflow/temp \
  --stagingLocation=gs://mazlum_dev/dataflow/staging \
  --serviceAccount=sa-dataflow-dev@gb-poc-373711.iam.gserviceaccount.com \
  --teamLeagueDataset=mazlum_test \
  --teamStatsTable=team_stat \
  --bqWriteMethod=FILE_LOADS \
  " \
  -Pdataflow-runner
```

### Streaming

```
mvn compile exec:java \
  -Dexec.mainClass=fr.groupbees.application.TeamLeagueApp \
  -Dexec.args=" \
  --project=gb-poc-373711 \
  --runner=DataflowRunner \
  --jobName=team-league-java-job-$(date +'%Y-%m-%d-%H-%M-%S') \
  --inputJsonFile=gs://mazlum_dev/team_league/input/json/input_teams_stats_raw.json \
  --inputSubscription=projects/gb-poc-373711/subscriptions/team_league \
  --region=europe-west1 \
  --streaming=true \
  --zone=europe-west1-d \
  --tempLocation=gs://mazlum_dev/dataflow/temp \
  --gcpTempLocation=gs://mazlum_dev/dataflow/temp \
  --stagingLocation=gs://mazlum_dev/dataflow/staging \
  --serviceAccount=sa-dataflow-dev@gb-poc-373711.iam.gserviceaccount.com \
  --teamLeagueDataset=mazlum_test \
  --teamStatsTable=team_stat \
  --bqWriteMethod=STREAMING_INSERTS \
  " \
  -Pdataflow-runner
```