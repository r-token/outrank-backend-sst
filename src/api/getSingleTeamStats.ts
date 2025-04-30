// src/api/getSingleTeamStats.ts
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

interface LatestStatsResult {
  team: string
  date: string
  stats: Record<string, string>
}

app.get('/', async (c) => {
  const team = c.req.query('team')
  
  if (!team) {
    return c.json({ message: 'Team name is required' }, 400)
  }
  
  try {
    // First, get the latest date for this team
    const dateResult = await client.send(new QueryCommand({
      TableName: Resource.AllRankings.name,
      KeyConditionExpression: "PK = :pk",
      ExpressionAttributeValues: {
        ":pk": { S: `team#${team}` }
      },
      ScanIndexForward: false,
      Limit: 1
    }))
    
    if (!dateResult.Items || dateResult.Items.length === 0) {
      return c.json({ message: 'Team data not found' }, 404)
    }
    
    // Extract the latest date
    const latestItem = dateResult.Items[0] as DynamoDBStatItem
    const skParts = latestItem.SK?.S?.split('#') || []
    const latestDate = skParts.length > 1 ? skParts[1] : ''
    
    // Now get all stats for this specific date
    const results = await client.send(new QueryCommand({
      TableName: Resource.AllRankings.name,
      KeyConditionExpression: "PK = :pk and begins_with(SK, :sk)",
      ExpressionAttributeValues: {
        ":pk": { S: `team#${team}` },
        ":sk": { S: `date#${latestDate}` }
      }
    }))
    
    // Transform results into object format
    const stats: Record<string, string> = {}
    results.Items?.forEach((item: DynamoDBStatItem) => {
      if (item.stat?.S && item.value?.N) {
        stats[item.stat.S] = item.value.N
      }
    })
    
    const response: LatestStatsResult = {
      team: team,
      date: latestDate,
      stats: stats
    }
    
    return c.json(response)
  } catch (error) {
    console.error('DynamoDB query error:', error)
    return c.json({ message: 'Internal server error' }, 500)
  }
})

export const handler = handle(app)