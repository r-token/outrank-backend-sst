// scripts/migrate.mjs - ES module version
import { DynamoDBClient, ScanCommand } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, BatchWriteCommand } from '@aws-sdk/lib-dynamodb';

const client = new DynamoDBClient({});
const ddbDocClient = DynamoDBDocumentClient.from(client);

// This is the inverse of your convertStatToDbAttribute function
const dbAttributeToStat = {
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

async function migrateData() {
  // Get table names from command line args or use defaults
  const args = process.argv.slice(2);
  const OLD_TABLE_NAME = args[0] || 'historicalRankingsTable';
  const NEW_TABLE_NAME = args[1] || 'AllRankings';
  
  console.log(`Starting migration from ${OLD_TABLE_NAME} to ${NEW_TABLE_NAME}`);
  
  // First, check if tables exist
  try {
    // Attempt to scan with limit 1 to check if table exists
    await client.send(new ScanCommand({
      TableName: OLD_TABLE_NAME,
      Limit: 1
    }));
    
    await client.send(new ScanCommand({
      TableName: NEW_TABLE_NAME,
      Limit: 1
    }));
    
    console.log("Both source and destination tables exist. Proceeding with migration...");
  } catch (error) {
    console.error("Error checking tables:", error.message);
    console.log("\nPossible issues:");
    console.log("1. One or both tables don't exist");
    console.log("2. AWS credentials are not configured correctly");
    console.log("3. You're not running in the correct AWS region");
    console.log("\nTo fix:");
    console.log("- Make sure both tables exist in your AWS account");
    console.log("- Run 'aws configure' to set up your credentials");
    console.log("- Check that you're using the correct table names");
    console.log("- If using SST dev, make sure 'sst dev' is running");
    process.exit(1);
  }
  
  try {
    let lastEvaluatedKey = undefined;
    let totalItemsProcessed = 0;
    let batchCount = 0;
    
    do {
      // Scan old table
      const scanParams = {
        TableName: OLD_TABLE_NAME,
        ExclusiveStartKey: lastEvaluatedKey,
        Limit: 100 // Process in smaller batches to avoid timeouts
      };
      
      const scanResult = await client.send(new ScanCommand(scanParams));
      
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
                SK: `date#${date}#stat#${statName}`,
                stat: statName,
                value: value,
                GSI1PK: `stat#${statName}`,
                GSI1SK: `date#${date}#team#${team}`
              }
            }
          });
        }
      }
      
      // Batch write to new table with parallelization
      const batchSize = 25; // DynamoDB maximum batch size
      const parallelBatches = 10; // Number of batches to process in parallel
      
      // Split items into batches of 25 (DynamoDB limit)
      const batches = [];
      for (let i = 0; i < newItems.length; i += batchSize) {
        const batch = newItems.slice(i, i + batchSize);
        batches.push(batch);
      }
      
      // Process batches in parallel groups
      for (let i = 0; i < batches.length; i += parallelBatches) {
        const batchGroup = batches.slice(i, i + parallelBatches);
        const batchPromises = batchGroup.map(batch => {
          return ddbDocClient.send(new BatchWriteCommand({
            RequestItems: {
              [NEW_TABLE_NAME]: batch
            }
          }));
        });
        
        // Wait for all batches in this group to complete
        await Promise.all(batchPromises);
        
        // Calculate progress metrics
        const startItem = i * batchSize;
        const endItem = Math.min((i + batchGroup.length) * batchSize, newItems.length);
        batchCount += batchGroup.length;
        
        // Log progress with correct item ranges
        console.log(`Batch group ${Math.floor(i/parallelBatches) + 1}: Processed ${batchGroup.length} batches (${batchCount} total)`); 
        console.log(`Items ${startItem + 1} to ${endItem} of ${newItems.length} (${endItem - startItem} items in this group)`); 
      }
      
      // Update the total items counter with actual items written to DynamoDB
      const itemsWrittenInThisBatch = newItems.length;
      totalItemsProcessed += itemsWrittenInThisBatch;
      lastEvaluatedKey = scanResult.LastEvaluatedKey;
      
      console.log(`Scan progress: ${scanResult.Items.length} items scanned in this batch`);
      console.log(`Migration progress: ${totalItemsProcessed} total items written to DynamoDB so far`);
      console.log(`---------------------------------------------------------`);
      
    } while (lastEvaluatedKey);
    
    console.log(`Migration complete! Total items processed: ${totalItemsProcessed}`);
  } catch (error) {
    console.error('Migration error:', error);
    process.exit(1);
  }
}

migrateData();
