#!/bin/bash
# Simple script to run the migration directly with Node.js

echo "Running migration..."
node scripts/migrate.mjs $1 $2
