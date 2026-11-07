# Setting up the stream manager

The user does these steps; agents never run setup or handle the secrets file.

## 1. Folders

Make three host folders and point `.env` (next to `compose.yaml`) at them:

```
SM_SECRETS_DIR=C:\Users\<you>\AppData\Local\OrgtreeStreamManager\secrets   # outside every Orgtree folder
SM_CONFIG_DIR=C:\Users\<you>\AppData\Local\OrgtreeStreamManager\config
SM_STATS_DIR=<org scratch>\stream-manager                                   # the stats app reads stats.json here
```

Copy `crates/sm-bin/config.example.toml` to `SM_CONFIG_DIR\config.toml` and fill in the
Twitch app's `client_id`, the channel's `broadcaster_id` and the bot's `bot_user_id`.
Make the bot account a moderator of the channel.

## 2. Secrets

Create `SM_SECRETS_DIR\secrets.toml`:

```toml
[twitch]
client_secret = "<from the Twitch developer console>"
refresh_token = ""            # filled in by step 3

[youtube]                     # only if YouTube is enabled; see section 3a
client_id = "<client id of the "TV and Limited Input devices" OAuth client>"
client_secret = "<that client's secret>"
refresh_token = ""

[streamlabs]                  # only if Streamlabs is enabled
socket_token = "<Streamlabs socket API token>"

[obs]
password = "<obs-websocket password>"

[[bridge_token]]
name = "listener-read"
token = "<stream-manager gen-token>"

[[bridge_token]]
name = "listener-control"
token = "<stream-manager gen-token>"
control = true
```

Make each token with `docker compose run --rm stream-manager gen-token`.

## 3. Sign-ins (device codes)

```
docker compose run --rm stream-manager setup twitch-bot
docker compose run --rm stream-manager setup twitch-broadcaster   # skip if the bot is the broadcaster
docker compose run --rm stream-manager setup youtube              # if enabled
```

Each prints an address and a code. Open the address, sign in as the account named, enter
the code, and copy the printed line into `secrets.toml` (the broadcaster's line is
`broadcaster_refresh_token`).

### 3a. YouTube (simulcast with Twitch)

YouTube runs next to Twitch: one bridge, one gate, one moderation pool; the disclosure goes out
on each chat once per live session. Chat comes in by server streaming (`chat_mode = "stream"`);
`chat_mode = "poll"` keeps the older poller as a fallback (set `budget_hours` for it).

1. Use the Google Cloud **project** you already have (its consent screen, its YouTube Data API
   v3 enablement and its quota). Add one new OAuth client in it, of type **TV and Limited Input
   devices**. A Desktop-app client cannot run the device flow.
2. Put that client's id and secret under `[youtube]` in `secrets.toml`.
3. Set `[youtube] enabled = true` in `config.toml`.
4. Run `setup youtube` (step 3). The sign-in asks only for the `youtube` scope, the one the
   device flow allows for live chat, moderation and ending the broadcast.
5. Publish the consent screen to **In production**. While it stays in Testing, Google expires
   the refresh token after 7 days. An unverified app shows Google's "unverified app" warning
   once when the channel owner signs in. If the sign-in ever expires, the stream manager
   raises the health item `youtube_auth` ("YouTube sign-in expired: run setup youtube again")
   and makes no further calls until `youtube.json` changes.

YouTube's default quota is 10,000 units a day, resetting at midnight Pacific. The stream
manager counts every call (`[youtube.costs]` holds the unit costs), shows the day's use in
`stats.json` (`youtube.quota_used`, `youtube.quota_day`), warns at 80% and 95%, and pauses its
chat replies on YouTube at 95% (moderation and the disclosure still go out).

## 4. Consent

```
docker compose run --rm -it stream-manager consent
```

The channel owner reads the list of automated actions and types `I AGREE`. The stream
manager will not run without this record.

## 5. Run

```
docker compose up -d
```

- In OBS, add a Browser source at `http://127.0.0.1:8788/overlay` (transparent), add
  media sources named as in `config.toml` (`SFX Chime`, `SFX Airhorn`), and make the
  `AFK` and `Live` scenes plus the `Fade` and `Cut` transitions.
- Turn on obs-websocket (Tools → WebSocket Server Settings) with a password.
- Give the listening agent its charter text and watchdogs: see `docs/listener.md`.
- Add the stats panel to the org from `apps/stats-panel` (`node apps/stats-panel/build.mjs`
  builds the single HTML file).

## Stopping the agents' chat without stopping the stream

Set `chat_say_enabled = false` in `config.toml` and restart the container.

## Letting agents end the stream

`stream.end` (OBS StopStream, and ending the YouTube broadcast) is refused unless
`config.toml` sets `stream_end_enabled = true` under `[bridge]`. A refused attempt returns
`disabled` and posts a `system.warning` with code `stream_end_disabled`.
