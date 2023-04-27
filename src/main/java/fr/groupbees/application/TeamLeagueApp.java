package fr.groupbees.application;

import com.google.api.services.bigquery.model.TableRow;
import fr.groupbees.domain.TeamBestPasserStats;
import fr.groupbees.domain.TeamStats;
import fr.groupbees.domain.TeamStatsRaw;
import fr.groupbees.domain.TeamTopScorerStats;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.beam.sdk.values.TypeDescriptor.of;

public class TeamLeagueApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(TeamLeagueApp.class);

    public static void main(String[] args) {
        final TeamLeagueOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(TeamLeagueOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read Json file", TextIO.read().from(options.getInputJsonFile()))
                .apply("Deserialize", MapElements.into(of(TeamStatsRaw.class)).via(TeamLeagueApp::deserializeToTeamStats))
                .apply("Validate fields", MapElements.into(of(TeamStatsRaw.class)).via(TeamStatsRaw::validateFields))
                .apply("Compute team stats", MapElements.into(of(TeamStats.class)).via(TeamStats::computeTeamStats))
                .apply("Add team slogan", MapElements.into(of(TeamStats.class)).via(TeamStats::addSloganToStats))
                .apply(BigQueryIO.<TeamStats>write()
                        .withMethod(options.getBqWriteMethod())
                        .to(options.getTeamLeagueDataset() + "." + options.getTeamStatsTable())
                        .withFormatFunction(TeamLeagueApp::toTeamStatsTableRow)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                );

        pipeline.run().waitUntilFinish();

        LOGGER.info("End of CDP integration case JOB");
    }

    private static TeamStatsRaw deserializeToTeamStats(final String teamStatsAsString) {
        return JsonUtil.deserialize(teamStatsAsString, TeamStatsRaw.class);
    }

    private static TableRow toTeamStatsTableRow(final TeamStats teamStats) {
        final TeamTopScorerStats topScorerStats = teamStats.getTopScorerStats();
        final TeamBestPasserStats bestPasserStats = teamStats.getBestPasserStats();

        final TableRow topScorerStatsRow = new TableRow()
                .set("firstName", topScorerStats.getFirstName())
                .set("lastName", topScorerStats.getLastName())
                .set("goals", topScorerStats.getGoals())
                .set("games", topScorerStats.getGames());

        final TableRow bestPasserStatsRow = new TableRow()
                .set("firstName", bestPasserStats.getFirstName())
                .set("lastName", bestPasserStats.getLastName())
                .set("goalAssists", bestPasserStats.getGoalAssists())
                .set("games", bestPasserStats.getGames());

        return new TableRow()
                .set("teamName", teamStats.getTeamName())
                .set("teamScore", teamStats.getTeamScore())
                .set("teamTotalGoals", teamStats.getTeamTotalGoals())
                .set("teamSlogan", teamStats.getTeamSlogan())
                .set("topScorerStats", topScorerStatsRow)
                .set("bestPasserStats", bestPasserStatsRow)
                .set("ingestionDate", new Instant().toString());
    }
}
