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
      fields: { team: "string", date: "string" },
      primaryIndex: { hashKey: "team", rangeKey: "date"}
    })

    new sst.aws.Function("getSingleTeamStats", {
      url: true,
      link: [allRankingsTable],
      handler: "src/api/getSingleTeamStats.handler"
    });
  }
})
