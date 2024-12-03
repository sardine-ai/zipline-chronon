FROM node:18-alpine

# Copy the built frontend to the new container
COPY ./frontend /app/frontend

# Set the working directory for subsequent commands
WORKDIR /app/frontend

# Expose port 3000 for the SvelteKit app
EXPOSE 3000

# Specify the command to run when the container starts
CMD ["node", "build/index.js"]
