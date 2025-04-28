/// <reference path="./.sst/platform/config.d.ts" />

// SST Config
export default $config({
  app(input) {
    return {
      name: "outrank-backend-sst",
      removal: input?.stage === "production" ? "retain" : "remove",
      protect: ["production"].includes(input?.stage),
      home: "aws",
    };
  },
  async run() {
    const allRankingsTable = new sst.aws.Dynamo("AllRankings", {
      fields: { 
        PK: "string",    // team#<team_name>
        SK: "string",    // date#<iso_date>
        GSI1PK: "string", // stat#<stat_name>
        GSI1SK: "string", // date#<iso_date>#team#<team_name>
      },
      primaryIndex: { hashKey: "PK", rangeKey: "SK" },
      globalIndexes: {
        StatDateTeamIndex: {
          hashKey: "GSI1PK",
          rangeKey: "GSI1SK",
          projection: "all"
        }
      }
    })

    // MARK: SCRAPER

    // Single stat scraper function
    const scrapeSingleStat = new sst.aws.Function("ScrapeSingleStat", {
      url: true,
      link: [allRankingsTable],
      handler: "src/scraper/scrapeSingleStat.handler",
      memory: "2 GB",
      timeout: "1 minute",
      nodejs: {
        install: ["@sparticuz/chromium"]
      },
      permissions: [
        {
          actions: ["ses:SendEmail"],
          resources: ["*"]
        }
      ]
    });

    // Step Functions state machine for orchestration
    const scraperStateMachine = new sst.aws.Function("ScraperStateMachine", {
      url: true,
      link: [allRankingsTable, scrapeSingleStat],
      handler: "src/scraper/orchestrator.handler",
      timeout: "15 minutes",
      permissions: [
        {
          actions: ["lambda:InvokeFunction", "ses:SendEmail"],
          resources: ["*"]
        }
      ]
    });

    // EventBridge rule to trigger daily scraping
    new sst.aws.Cron("DailyScraperTrigger", {
      schedule: "cron(0 14 * * ? *)", // 2 PM UTC, 8 AM MDT
      job: scraperStateMachine.arn
    });

    // MARK: API

    new sst.aws.Function("getSingleTeamStats", {
      url: true,
      link: [allRankingsTable],
      handler: "src/api/getSingleTeamStats.handler"
    });

    new sst.aws.Function("getStatsByTeam", {
      url: true,
      link: [allRankingsTable],
      handler: "src/api/getStatsByTeam.handler"
    });

    new sst.aws.Function("getSingleTeamHistoricalStats", {
      url: true,
      link: [allRankingsTable],
      handler: "src/api/getSingleTeamHistoricalStats.handler"
    });

    // MARK: MIGRATION
    new sst.aws.Function("MigrateHistoricalData", {
      handler: "src/migration/migrateHistoricalDataTable.handler",
      timeout: "15 minutes",
      environment: {
        NEW_TABLE_NAME: allRankingsTable.name
      },
      permissions: [
        {
          actions: ["dynamodb:Scan", "dynamodb:BatchWriteItem"],
          resources: ["*"]
        }
      ]
    });
  }
})