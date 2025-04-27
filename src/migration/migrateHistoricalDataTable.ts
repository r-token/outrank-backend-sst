// src/migration/migrateHistoricalData.ts
import { DynamoDBClient, ScanCommand, ScanCommandInput, ScanCommandOutput, AttributeValue } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, BatchWriteCommand } from '@aws-sdk/lib-dynamodb'
import { Handler } from 'aws-lambda';

const client = new DynamoDBClient({})
const ddbDocClient = DynamoDBDocumentClient.from(client)

// This is the inverse of your convertStatToDbAttribute function
const dbAttributeToStat: Record<string, string> = {
  'ThirdDownConversionPct': '3rd Down Conversion Pct',
  'ThirdDownConversionPctDefense': '3rd Down Conversion Pct Defense',
  'FourthDownConversionPct': '4th Down Conversion Pct',
  'FourthDownConversionPctDefense': '4th Down Conversion Pct Defense',
  'BlockedKicks': 'Blocked Kicks',
  'BlockedKicksAllowed': 'Blocked Kicks Allowed',
  'BlockedPunts': 'Blocked Punts',
  'BlockedPuntsAllowed': 'Blocked Punts Allowed',
  'CompletionPercentage': 'Completion Percentage',
  'DefensiveTDs': 'Defensive TDs',
  'FewestPenalties': 'Fewest Penalties',
  'FewestPenaltiesPerGame': 'Fewest Penalties Per Game',
  'FewestPenaltyYards': 'Fewest Penalty Yards',
  'FewestPenaltyYardsPerGame': 'Fewest Penalty Yards Per Game',
  'FirstDownsDefense': 'First Downs Defense',
  'FirstDownsOffense': 'First Downs Offense',
  'FumblesLost': 'Fumbles Lost',
  'FumblesRecovered': 'Fumbles Recovered',
  'KickoffReturnDefense': 'Kickoff Return Defense',
  'KickoffReturns': 'Kickoff Returns',
  'NetPunting': 'Net Punting',
  'PassesHadIntercepted': 'Passes Had Intercepted',
  'PassesIntercepted': 'Passes Intercepted',
  'PassingOffense': 'Passing Offense',
  'PassingYardsAllowed': 'Passing Yards Allowed',
  'PassingYardsPerCompletion': 'Passing Yards Per Completion',
  'PuntReturnDefense': 'Punt Return Defense',
  'PuntReturns': 'Punt Returns',
  'RedZoneDefense': 'Red Zone Defense',
  'RedZoneOffense': 'Red Zone Offense',
  'RushingDefense': 'Rushing Defense',
  'RushingOffense': 'Rushing Offense',
  'SacksAllowed': 'Sacks Allowed',
  'ScoringDefense': 'Scoring Defense',
  'ScoringOffense': 'Scoring Offense',
  'TacklesForLossAllowed': 'Tackles For Loss Allowed',
  'TeamPassingEfficiency': 'Team Passing Efficiency',
  'TeamPassingEfficiencyDefense': 'Team Passing Efficiency Defense',
  'TeamSacks': 'Team Sacks',
  'TeamTacklesForLoss': 'Team Tackles For Loss',
  'TimeOfPossession': 'Time Of Possession',
  'TotalDefense': 'Total Defense',
  'TotalOffense': 'Total Offense',
  'TurnoverMargin': 'Turnover Margin',
  'TurnoversGained': 'Turnovers Gained',
  'TurnoversLost': 'Turnovers Lost',
  'WinningPercentage': 'Winning Percentage'
};

export const handler: Handler = async () => {
  const OLD_TABLE_NAME = 'historicalRankingsTable';
  const NEW_TABLE_NAME = process.env.NEW_TABLE_NAME || 'AllRankings';
  
  try {
    console.log('Starting migration from historicalRankingsTable to AllRankings');
    
    let lastEvaluatedKey: Record<string, AttributeValue> | undefined;
    let totalItemsProcessed = 0;
    let batchCount = 0;
    
    do {
      // Scan old table
      const scanParams: ScanCommandInput = {
        TableName: OLD_TABLE_NAME,
        ExclusiveStartKey: lastEvaluatedKey,
        Limit: 100 // Process in smaller batches to avoid timeouts
      };
      
      const scanResult: ScanCommandOutput = await client.send(new ScanCommand(scanParams));
      
      if (!scanResult.Items || scanResult.Items.length === 0) {
        console.log('No more items to process');
        break;
      }
      
      // Transform items for new table format
      const newItems = [];
      for (const item of scanResult.Items) {
        const team = item.team?.S;
        const date = item.date?.S;
        
        if (!team || !date) {
          console.warn('Missing team or date in item, skipping...');
          continue;
        }
        
        // Process each stat field
        for (const [dbAttribute, dynamoValue] of Object.entries(item)) {
          if (dbAttribute === 'team' || dbAttribute === 'date') continue;
          
          const statName = dbAttributeToStat[dbAttribute];
          if (!statName) {
            console.warn(`Unknown db attribute: ${dbAttribute}, skipping...`);
            continue;
          }
          
          // Get the numeric value - check if the attribute exists and has N property
          let value = 99999;
          if (dynamoValue && 'N' in dynamoValue && dynamoValue.N) {
            value = parseInt(dynamoValue.N);
          }
          
          newItems.push({
            PutRequest: {
              Item: {
                PK: `team#${team}`,
                SK: `date#${date}`,
                stat: statName,
                value: value,
                GSI1PK: `stat#${statName}`,
                GSI1SK: `date#${date}#team#${team}`
              }
            }
          });
        }
      }
      
      // Batch write to new table
      const batchSize = 25;
      for (let i = 0; i < newItems.length; i += batchSize) {
        const batch = newItems.slice(i, i + batchSize);
        
        await ddbDocClient.send(new BatchWriteCommand({
          RequestItems: {
            [NEW_TABLE_NAME]: batch
          }
        }));
        
        batchCount++;
        console.log(`Processed batch ${batchCount}, items ${i} to ${i + batch.length}`);
      }
      
      totalItemsProcessed += scanResult.Items.length;
      lastEvaluatedKey = scanResult.LastEvaluatedKey;
      
      console.log(`Processed ${totalItemsProcessed} items so far...`);
      
    } while (lastEvaluatedKey);
    
    console.log(`Migration complete! Total items processed: ${totalItemsProcessed}`);
    
    return {
      statusCode: 200,
      body: JSON.stringify({
        message: 'Migration completed successfully',
        totalItemsProcessed,
        batchCount
      })
    };
    
  } catch (error) {
    console.error('Migration error:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: 'Migration failed',
        error: error instanceof Error ? error.message : 'Unknown error'
      })
    };
  }
};