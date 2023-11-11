#!/bin/bash

# Project Name
PROJECT_NAME="QualityTool"

# Project Directory
PROJECT_DIRECTORY="/home/marcos_romero/$PROJECT_NAME"

# Create main directory
mkdir -p "$PROJECT_DIRECTORY"

# Subdirectories
DIRECTORIES=("events/producers" "events/processors" "events/consumers" "persistence" "integration/adapters" "integration/connectors" "security" "monitoring/logs" "config/secrets")

# Create subdirectories
for DIRECTORY in "${DIRECTORIES[@]}"; do
  mkdir -p "$PROJECT_DIRECTORY/$DIRECTORY"
done

# Create files
touch "$PROJECT_DIRECTORY/events/__init__.py"
touch "$PROJECT_DIRECTORY/integration/__init__.py"

# Create README.md file
echo "# $PROJECT_NAME" >> "$PROJECT_DIRECTORY/README.md"

# Create requisitos.txt file
touch "$PROJECT_DIRECTORY/requirements.txt"

# Create main.py file
touch "$PROJECT_DIRECTORY/main.py"

echo "Directory structure created at: $PROJECT_DIRECTORY"

