#!/bin/bash

# Clean any existing Docker image
docker rmi acp_submission:latest || true

# Build the Docker image
docker build -t acp_submission .

# Save the Docker image to a file
docker save acp_submission:latest > acp_submission_image.tar

echo "Docker image built and saved to acp_submission_image.tar"