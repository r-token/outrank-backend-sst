import { Hono } from 'hono'
import { Resource } from "sst";
import { DynamoDBClient, QueryCommand } from '@aws-sdk/client-dynamodb'
import { handle } from 'hono/aws-lambda'

const app = new Hono()
const client = new DynamoDBClient({})

// Define types for better type safety
interface DynamoDBStatItem {
  PK?: { S: string }
  SK?: { S: string }
  stat?: { S: string }
  value?: { N: string }
  GSI1PK?: { S: string }
  GSI1SK?: { S: string }
}

interface TeamStatResult {
  team: string
  date: string
  value: string
}

app.get('/', async (c) => {
  const stat = c.req.query('stat')
  
  if (!stat) {
    return c.json({ message: 'Stat name is required' }, 400)
  }
  
  try {
    const results = await client.send(new QueryCommand({
      TableName: Resource.AllRankings.name,
      IndexName: 'StatDateTeamIndex',
      KeyConditionExpression: "GSI1PK = :pk",
      ExpressionAttributeValues: {
        ":pk": { S: `stat#${stat}` }
      },
      ScanIndexForward: false // Get most recent first
    }))
    
    if (!results.Items || results.Items.length === 0) {
      return c.json({ message: 'Stat data not found' }, 404)
    }
    
    // Get latest value for each team
    const latestByTeam = new Map<string, TeamStatResult>()
    
    results.Items.forEach((item: DynamoDBStatItem) => {
      // Safely extract team and date from GSI1SK
      if (item.GSI1SK?.S && item.value?.N) {
        const skParts = item.GSI1SK.S.split('#')
        if (skParts.length >= 4) {
          const date = skParts[1] || ''
          const team = skParts[3] || ''
          
          if (team && !latestByTeam.has(team)) {
            latestByTeam.set(team, {
              team: team,
              date: date,
              value: item.value.N
            })
          }
        }
      }
    })
    
    // Convert Map to array
    const response = Array.from(latestByTeam.values())
    
    return c.json(response)
  } catch (error) {
    console.error('DynamoDB query error:', error)
    return c.json({ message: 'Internal server error' }, 500)
  }
})

export const handler = handle(app)