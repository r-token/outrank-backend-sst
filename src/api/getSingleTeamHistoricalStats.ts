// src/api/getSingleTeamHistoricalStats.ts
import { Hono } from 'hono'
import { Resource } from "sst";
import { DynamoDBClient, QueryCommand } from '@aws-sdk/client-dynamodb'
import { handle } from 'hono/aws-lambda'

const app = new Hono()
const client = new DynamoDBClient({})

interface DynamoDBStatItem {
  PK?: { S: string }
  SK?: { S: string }
  stat?: { S: string }
  value?: { N: string }
}

interface HistoricalStatsResult {
  team: string
  startDate: string
  endDate: string
  statsByDate: Record<string, Record<string, string>>
}

app.get('/', async (c) => {
  const team = c.req.query('team')
  const startDate = c.req.query('startDate')
  const endDate = c.req.query('endDate')
  
  if (!team) {
    return c.json({ message: 'Team name is required' }, 400)
  }
  if (!startDate || !endDate) {
    return c.json({ message: 'Start date and end date are required' }, 400)
  }
  
  try {
    const results = await client.send(new QueryCommand({
      TableName: Resource.AllRankings.name,
      KeyConditionExpression: "PK = :pk and begins_with(SK, :skPrefix)",
      FilterExpression: "SK between :startSk and :endSk",
      ExpressionAttributeValues: {
        ":pk": { S: `team#${team}` },
        ":skPrefix": { S: "date#" },
        ":startSk": { S: `date#${startDate}` },
        ":endSk": { S: `date#${endDate}#zzz` }
      }
    }))
    
    if (!results.Items || results.Items.length === 0) {
      return c.json({ message: 'No historical data found for the specified date range' }, 404)
    }
    
    // Group stats by date
    const statsByDate: Record<string, Record<string, string>> = {}
    
    results.Items.forEach((item: DynamoDBStatItem) => {
      if (item.SK?.S && item.stat?.S && item.value?.N) {
        const skParts = item.SK.S.split('#')
        const dateStr = skParts.length > 1 ? skParts[1] : ''
        
        if (!statsByDate[dateStr]) {
          statsByDate[dateStr] = {}
        }
        
        statsByDate[dateStr][item.stat.S] = item.value.N
      }
    })
    
    const response: HistoricalStatsResult = {
      team: team,
      startDate: startDate,
      endDate: endDate,
      statsByDate: statsByDate
    }
    
    return c.json(response)
  } catch (error) {
    console.error('DynamoDB query error:', error)
    return c.json({ message: 'Internal server error' }, 500)
  }
})

export const handler = handle(app)