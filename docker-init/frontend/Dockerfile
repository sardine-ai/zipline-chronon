FROM base_image:latest AS build_env

# Copy the entire project directory into the container
COPY . /app

# Install Node.js and npm
RUN apk add --update nodejs npm

# Build the frontend using SBT
RUN sbt "project frontend" buildFrontend

FROM node:18-alpine

# Copy the built frontend from the previous stage to the new container
COPY --from=build_env app/frontend /app/frontend

# Set the working directory for subsequent commands
WORKDIR /app/frontend

# Expose port 3000 for the SvelteKit app
EXPOSE 3000

# Specify the command to run when the container starts
CMD ["node", "build/index.js"]
