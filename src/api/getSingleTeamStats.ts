import { Hono } from 'hono'
import { Resource } from "sst";
import { DynamoDBClient, QueryCommand } from '@aws-sdk/client-dynamodb'
import { handle } from 'hono/aws-lambda'

const app = new Hono()
const client = new DynamoDBClient({})

app.get('/', async (c) => {
  const team = c.req.query('team')
  const date = c.req.query('date')
  
  if (!team) {
    return c.json({ message: 'Team name is required' }, 400)
  }
  if (!date) {
    return c.json({ message: 'Date is required' }, 400)
  }
  
  const results = await client.send(new QueryCommand({
    TableName : Resource.AllRankings.name,
		KeyConditionExpression: "team = :team and #dynamo_date >= :date",
		ExpressionAttributeValues: {
			":team": { S: team },
			":date": { S: date }
		},
		ExpressionAttributeNames: { "#dynamo_date": "date" },
		Limit: 1
  }))
  
  if (!results.Items || results.Items.length === 0) {
    return c.json({ message: 'Team data not found' }, 404)
  }
  
  return c.json(results.Items[0])
})

export const handler = handle(app)
