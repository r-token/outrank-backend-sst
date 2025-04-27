// src/scraper/orchestrator.ts
import { Handler } from 'aws-lambda';
import { SESClient, SendEmailCommand } from "@aws-sdk/client-ses";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { Resource } from "sst";

const sesClient = new SESClient({});
const lambdaClient = new LambdaClient({});

const allStats = [
    '3rd Down Conversion Pct',
    '3rd Down Conversion Pct Defense',
    '4th Down Conversion Pct',
    '4th Down Conversion Pct Defense',
    'Blocked Kicks',
    'Blocked Kicks Allowed',
    'Blocked Punts',
    'Blocked Punts Allowed',
    'Completion Percentage',
    'Defensive TDs',
    'Fewest Penalties',
    'Fewest Penalties Per Game',
    'Fewest Penalty Yards',
    'Fewest Penalty Yards Per Game',
    'First Downs Defense',
    'First Downs Offense',
    'Fumbles Lost',
    'Fumbles Recovered',
    'Kickoff Return Defense',
    'Kickoff Returns',
    'Net Punting',
    'Passes Had Intercepted',
    'Passes Intercepted',
    'Passing Offense',
    'Passing Yards Allowed',
    'Passing Yards Per Completion',
    'Punt Return Defense',
    'Punt Returns',
    'Red Zone Defense',
    'Red Zone Offense',
    'Rushing Defense',
    'Rushing Offense',
    'Sacks Allowed',
    'Scoring Defense',
    'Scoring Offense',
    'Tackles For Loss Allowed',
    'Team Passing Efficiency',
    'Team Passing Efficiency Defense',
    'Team Sacks',
    'Team Tackles For Loss',
    'Time Of Possession',
    'Total Defense',
    'Total Offense',
    'Turnover Margin',
    'Turnovers Gained',
    'Turnovers Lost',
    'Winning Percentage'
]

export const handler: Handler = async () => {
  const date = new Date().toISOString();
  const startTime = new Date();
  const results: { stat: string; success: boolean; error?: string }[] = [];
  
  try {
    // Create promises for all stats
    const statPromises = allStats.map(async (stat) => {
      try {
        const response = await lambdaClient.send(new InvokeCommand({
          FunctionName: Resource.ScrapeSingleStat.name,
          Payload: JSON.stringify({ stat, date })
        }));
        
        const result = JSON.parse(new TextDecoder().decode(response.Payload));
        return { stat, success: true };
      } catch (error) {
        console.error(`Error scraping ${stat}:`, error);
        return { stat, success: false, error: (error as Error).message };
      }
    });
    
    // Wait for all stats to complete
    results.push(...await Promise.all(statPromises));
    
    // Send summary email
    const endTime = new Date();
    const failures = results.filter(r => !r.success);
    
    await sendSummaryEmail(failures.length === 0, startTime, endTime, failures);
    
    return {
      statusCode: 200,
      body: JSON.stringify({
        totalStats: allStats.length,
        successCount: results.filter(r => r.success).length,
        failures
      })
    };
    
  } catch (error) {
    console.error('Orchestrator error:', error);
    await sendSummaryEmail(false, startTime, new Date(), [], error as Error);
    throw error;
  }
};

async function sendSummaryEmail(
  success: boolean,
  startTime: Date,
  endTime: Date,
  failures: { stat: string; error?: string }[] = [],
  overallError?: Error
) {
  const subject = success ? "Scrape Successful" : "Scrape Failed";
  const message = overallError
    ? `Overall error: ${overallError.message}`
    : failures.length > 0
    ? `Failed stats: ${failures.map(f => `${f.stat} - ${f.error}`).join('\n')}`
    : "All stats successfully scraped";

  await sesClient.send(new SendEmailCommand({
    Source: "outrankemailer@gmail.com",
    Destination: {
      ToAddresses: ["ryantoken13@gmail.com"]
    },
    Message: {
      Subject: { Data: subject },
      Body: {
        Html: {
          Data: `
            <h2>${subject}</h2>
            <p>Start Time: ${startTime}</p>
            <p>End Time: ${endTime}</p>
            <p>${message}</p>
          `
        }
      }
    }
  }));
}