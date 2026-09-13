# Pollify — Real-time polling web application

A polished full-stack implementation of the internship developer task.

## Stack
- Frontend: React + Vite
- Backend: Go + Gin
- Database: MongoDB
- Realtime: Redis Pub/Sub + WebSockets
- Auth: JWT + bcrypt

## Features
- Sign up / sign in before creating polls
- Server-side validation for questions/options
- 2–10 poll options with unique values
- Single-choice or multiple-choice polls
- Optional poll expiry
- Public shareable `/p/<slug>` URLs
- One vote per browser using an HttpOnly voter cookie
- Live result updates through Redis Pub/Sub and WebSockets
- Explore recent polls
- Responsive UI matching the supplied Pollify design direction
- Docker Compose for the complete stack

## Run with Docker
Install Docker Desktop, then from this directory:

```bash
docker compose up --build
```

Open:
- Frontend: http://localhost:5173
- API health: http://localhost:8080/health

## Run without Docker
Start MongoDB and Redis first.

Backend:
```bash
cd backend
go mod download
go run .
```

Frontend:
```bash
cd frontend
npm install
npm run dev
```

For a different API URL, copy `frontend/.env.example` to `frontend/.env` and set `VITE_API_URL`.

## Production
Deploy the frontend and backend separately (or use containers) with managed MongoDB and Redis. Set:
`MONGO_URI`, `MONGO_DB`, `REDIS_ADDR`, `JWT_SECRET`, and `FRONTEND_URL`.

The frontend's nginx config includes SPA fallback so direct `/p/<slug>` links work after deployment.
