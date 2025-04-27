/// <reference path="./.sst/platform/config.d.ts" />

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
          projection: "all" // This will include all attributes in the index
        }
      }
    })

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

    new sst.aws.Function("getTeamHistoricalStats", {
      url: true,
      link: [allRankingsTable],
      handler: "src/api/getTeamHistoricalStats.handler"
    });
  }
})