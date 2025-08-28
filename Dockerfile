cat > docker-compose.yml <<'EOF'
version: "3.9"

services:
  scraper:
    build: .
    container_name: scraper
    ports:
      - "8000:8000"
    environment:
      - SCRAPER_API_KEY=supersecret123   # change in production
    volumes:
      - ./exports:/app/exports
    restart: unless-stopped
