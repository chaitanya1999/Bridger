# Bridger

A simple Heroku-ready Express app for proxying API calls.

## What it does

- Serves `public/index.html` as a small playground.
- Accepts `POST /bridge` requests with:

```json
{
  "endpoint": "https://api.example.com/path",
  "method": "POST",
  "headers": {
    "content-type": "application/json"
  },
  "body": {
    "hello": "world"
  }
}
```

- Calls the provided `endpoint`.
- Returns the upstream response status, headers, and body.

## Run locally

```bash
npm install
npm start
```

Open `http://localhost:3000`.

For development:

```bash
npm run dev
```

## Deploy to Heroku

```bash
heroku create your-bridger-app
git init
git add .
git commit -m "Create bridger"
git push heroku main
```

Heroku sets `PORT` automatically. You can set `JSON_LIMIT` if you need larger request payloads:

```bash
heroku config:set JSON_LIMIT=25mb
```
