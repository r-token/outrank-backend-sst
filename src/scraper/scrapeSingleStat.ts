// src/scraper/scrapeSingleStat.ts
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, BatchWriteCommand } from '@aws-sdk/lib-dynamodb'
import chromium from '@sparticuz/chromium'
import puppeteer from 'puppeteer-core'
import { Resource } from "sst";
import { Handler, APIGatewayProxyEventV2 } from 'aws-lambda';
import { browser } from 'process';

const client = new DynamoDBClient({})
const ddbDocClient = DynamoDBDocumentClient.from(client)

// Your local Chromium path for development
const YOUR_LOCAL_CHROMIUM_PATH = "/tmp/localChromium/chromium/mac_arm-1452359/chrome-mac/Chromium.app/Contents/MacOS/Chromium";

interface ScrapeEvent {
  stat: string;
  date?: string;
}

interface TeamRanking {
  rank: string;
  team: string;
}

export const handler: Handler = async (event) => {
  let stat: string;
  let date: string;

  if (isAPIGatewayEvent(event)) {
    // Function URL invocation
    const queryParams = event.queryStringParameters || {};
    stat = queryParams.stat || '';
    date = queryParams.date || new Date().toISOString();

    if (!stat) {
      return {
        statusCode: 400,
        body: JSON.stringify({ message: 'stat parameter is required' })
      };
    }
  } else {
    // Direct Lambda invocation (from orchestrator)
    const lambdaEvent = event as ScrapeEvent;
    stat = lambdaEvent.stat;
    date = lambdaEvent.date || new Date().toISOString();
  }

  let browser = null

  try {
    browser = await puppeteer.launch({
        args: [
          ...chromium.args,
          '--no-sandbox',
          '--disable-setuid-sandbox',
          '--disable-dev-shm-usage',
          '--disable-gpu',
          '--disable-blink-features=AutomationControlled',  // Hide automation
          '--window-size=1920,1080'
        ],
        defaultViewport: chromium.defaultViewport,
        executablePath: process.env.SST_DEV
          ? YOUR_LOCAL_CHROMIUM_PATH
          : await chromium.executablePath(),
        headless: chromium.headless,
    });
    
    const page = await browser.newPage();
    
    // Enhanced browser configuration
    await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    );
    
    // Add extra headers to appear more like a real browser
    await page.setExtraHTTPHeaders({
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    });
      
    // Enable JavaScript
    await page.setJavaScriptEnabled(true);
    
    // Increase timeouts
    page.setDefaultNavigationTimeout(60000);
    page.setDefaultTimeout(30000);

    const url = getUrlForStatistic(stat);
    console.log(`Scraping ${stat} from ${url}`);

    // Get rankings with retry logic
    let fullRankings: TeamRanking[] = [];

    for (let attempt = 0; attempt < 3; attempt++) {
      if (!url) {
        throw new Error(`Failed to get URL for statistic: ${stat}`);
      }
      fullRankings = await scrapeAllPages(page, url);

      if (fullRankings.length > 0 && fullRankings[0].rank === '1') {
        break;
      }

      console.log(`Scrape attempt ${attempt + 1} failed, retrying...`);
      fullRankings = [];
    }

    if (fullRankings.length === 0) {
      throw new Error(`Failed to scrape data for ${stat}`);
    }

    // Process rankings and prepare for DynamoDB
    const processedRankings = processRankings(fullRankings);

    // Batch write to DynamoDB
    await writeToDynamoDB(processedRankings, stat, date);

    return {
      statusCode: 200,
      body: JSON.stringify({
        stat,
        date,
        teamsProcessed: processedRankings.length
      })
    };
  } catch (error) {
    console.error(`Error scraping ${stat}:`, error);
    if (isAPIGatewayEvent(event)) {
      return {
        statusCode: 500,
        body: JSON.stringify({
          message: 'Error scraping stat',
          error: error instanceof Error ? error.message : 'Unknown error'
        })
      };
    } else {
      throw error; // For orchestrator to handle
    }
  } finally {
    if (browser) {
      await browser.close();
    }
  }
};

async function scrapeAllPages(page: any, baseUrl: string): Promise<TeamRanking[]> {
  const allRankings: TeamRanking[] = [];
  
  for (let pageNum = 1; pageNum <= 3; pageNum++) {
    const pageUrl = pageNum === 1 ? baseUrl : `${baseUrl}/p${pageNum}`;
    const rankings = await scrapePage(page, pageUrl);
    allRankings.push(...rankings);
  }
  
  return allRankings;
}

async function scrapePage(page: any, url: string): Promise<TeamRanking[]> {
    try {
      // Use a more forgiving navigation approach
      await page.goto(url, { 
        waitUntil: 'domcontentloaded',  // Changed from networkidle0 to be less strict
        timeout: 45000  // Increased timeout
      });
      
      // Wait for the specific table element with increased timeout
      try {
        await page.waitForSelector('.block-stats__stats-table', { 
          timeout: 20000,
          visible: true 
        });
      } catch (selectorError) {
        console.log('Primary selector failed, checking page content...');
        
        // Check if we're on a blocked/error page
        const pageContent = await page.content();
        if (pageContent.includes('Access Denied') || pageContent.includes('Just a moment...')) {
          throw new Error('Access blocked by the website');
        }
        
        // Try alternative selector strategies
        const hasTable = await page.evaluate(() => {
          return !!document.querySelector('table.block-stats__stats-table');
        });
        
        if (!hasTable) {
          throw new Error('Table not found on page');
        }
      }
      
      // Extract data with more error handling
      const rankings = await page.evaluate(() => {
        const table = document.querySelector('.block-stats__stats-table');
        if (!table) return [];
        
        const rows = table.querySelectorAll('tbody > tr');
        const data: TeamRanking[] = [];
        
        for (const row of rows) {
          const cells = row.querySelectorAll('td');
          if (cells.length >= 2) {
            const rank = cells[0].textContent?.trim() || '-';
            const team = cells[1].textContent?.trim() || '';
            
            if (team && /^[a-zA-Z]/.test(team)) {
              data.push({ rank, team });
            }
          }
        }
        
        return data;
      });
      
      console.log(`Successfully scraped ${rankings.length} teams from ${url}`);
      return rankings;
      
    } catch (error) {
      console.error(`Error scraping page ${url}:`, error);
      
      // Enhanced error debugging
      try {
        const screenshot = await page.screenshot({ encoding: 'base64' });
        console.log('Error screenshot taken - base64 length:', screenshot.length);
        
        const html = await page.content();
        console.log('Page HTML length:', html.length);
        console.log('Page HTML preview:', html.substring(0, 1000));
        
        // Check for specific error conditions
        if (html.includes('Access Denied')) {
          console.error('Access denied by the website');
        }
        if (html.includes('Just a moment...')) {
          console.error('Cloudflare protection detected');
        }
      } catch (debugError) {
        console.error('Debug logging failed:', debugError);
      }
      
      return [];
    }
}

function processRankings(rankings: TeamRanking[]): TeamRanking[] {
  // Process ties
  for (let i = 0; i < rankings.length; i++) {
    let tieCounter = i;
    
    while (rankings[i].rank === '-' && tieCounter > 0) {
      rankings[i].rank = rankings[tieCounter - 1].rank;
      tieCounter--;
    }
  }
  
  return rankings;
}

async function writeToDynamoDB(rankings: TeamRanking[], stat: string, date: string) {
  const batchSize = 25; // DynamoDB batch limit
  const items = rankings.map(({ team, rank }) => ({
    PutRequest: {
      Item: {
        PK: `team#${team}`,
        SK: `date#${date}`,
        stat,
        value: parseInt(rank) || 99999,
        GSI1PK: `stat#${stat}`,
        GSI1SK: `date#${date}#team#${team}`
      }
    }
  }));
  
  // Split into batches
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    await ddbDocClient.send(new BatchWriteCommand({
      RequestItems: {
        [Resource.AllRankings.name]: batch
      }
    }));
  }
}

// Keep your existing getUrlForStatistic function
function getUrlForStatistic(stat: string) {
    const baseUrl = 'https://www.ncaa.com/stats/football/fbs/current/team'
    switch (stat) {
      case '3rd Down Conversion Pct':
        return `${baseUrl}/699`
      case '3rd Down Conversion Pct Defense':
        return `${baseUrl}/701`
      case '4th Down Conversion Pct':
        return `${baseUrl}/700`
      case '4th Down Conversion Pct Defense':
        return `${baseUrl}/702`
      case 'Blocked Kicks':
        return `${baseUrl}/785`
      case 'Blocked Kicks Allowed':
        return `${baseUrl}/786`
      case 'Blocked Punts':
        return `${baseUrl}/790`
      case 'Blocked Punts Allowed':
        return `${baseUrl}/791`
      case 'Completion Percentage':
        return `${baseUrl}/756`
      case 'Defensive TDs':
        return `${baseUrl}/926`
      case 'Fewest Penalties':
        return `${baseUrl}/876`
      case 'Fewest Penalties Per Game':
        return `${baseUrl}/697`
      case 'Fewest Penalty Yards':
        return `${baseUrl}/877`
      case 'Fewest Penalty Yards Per Game':
        return `${baseUrl}/698`
      case 'First Downs Defense':
        return `${baseUrl}/694`
      case 'First Downs Offense':
        return `${baseUrl}/693`
      case 'Fumbles Lost':
        return `${baseUrl}/458`
      case 'Fumbles Recovered':
        return `${baseUrl}/456`
      case 'Kickoff Return Defense':
        return `${baseUrl}/463`
      case 'Kickoff Returns':
        return `${baseUrl}/96`
      case 'Net Punting':
        return `${baseUrl}/98`
      case 'Passes Had Intercepted':
        return `${baseUrl}/459`
      case 'Passes Intercepted':
        return `${baseUrl}/457`
      case 'Passing Offense':
        return `${baseUrl}/25`
      case 'Passing Yards Allowed':
        return `${baseUrl}/695`
      case 'Passing Yards Per Completion':
        return `${baseUrl}/741`
      case 'Punt Return Defense':
        return `${baseUrl}/462`
      case 'Punt Returns':
        return `${baseUrl}/97`
      case 'Red Zone Defense':
        return `${baseUrl}/704`
      case 'Red Zone Offense':
        return `${baseUrl}/703`
      case 'Rushing Defense':
        return `${baseUrl}/24`
      case 'Rushing Offense':
        return `${baseUrl}/23`
      case 'Sacks Allowed':
        return `${baseUrl}/468`
      case 'Scoring Defense':
        return `${baseUrl}/28`
      case 'Scoring Offense':
        return `${baseUrl}/27`
      case 'Tackles For Loss Allowed':
        return `${baseUrl}/696`
      case 'Team Passing Efficiency':
        return `${baseUrl}/465`
      case 'Team Passing Efficiency Defense':
        return `${baseUrl}/40`
      case 'Team Sacks':
        return `${baseUrl}/466`
      case 'Team Tackles For Loss':
        return `${baseUrl}/467`
      case 'Time Of Possession':
        return `${baseUrl}/705`
      case 'Total Defense':
        return `${baseUrl}/22`
      case 'Total Offense':
        return `${baseUrl}/21`
      case 'Turnover Margin':
        return `${baseUrl}/29`
      case 'Turnovers Gained':
        return `${baseUrl}/460`
      case 'Turnovers Lost':
        return `${baseUrl}/461`
      case 'Winning Percentage':
        return `${baseUrl}/742`
    }
}

function isAPIGatewayEvent(event: any): event is APIGatewayProxyEventV2 {
    return event && event.requestContext && event.requestContext.http;
  }