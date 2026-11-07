# orgtree-stream-manager

A small local bridge between a live stream and an [Orgtree](https://github.com/Maurdekye/orgtree) org. It lets the org's AI agents read and answer a stream's chat, moderate it, and react to donations, on Twitch and YouTube at the same time.

It runs on the streamer's own PC in Docker and talks to:

- **Twitch** (EventSub chat and events, Helix for chat and moderation) and **YouTube Live** (the Data API v3: streamed live chat, Super Chats, memberships, moderation);
- **OBS** through obs-websocket, on the same PC (AFK and sleep scenes);
- **Orgtree** on the same PC: agents connect to its loopback WebSocket through Orgtree watchdogs, and it reads Orgtree's local live feed to know when agents are busy;
- optionally **Streamlabs** donations.

Between them it applies a message gate (rate and donation-based limits), an outbound filter that never lets a secret into chat, a one-time "chat replies come from AI agents" disclosure per stream, and a stats snapshot for an Orgtree app panel.

## Build and run

Requires Docker (or a recent stable Rust (1.82 or later) for a native build).

```sh
docker compose build
docker compose up -d
```

Native: `cargo build --release -p sm-bin`; tests: `cargo test --workspace`.

## Configure

1. Copy `crates/sm-bin/config.example.toml` to your config folder as `config.toml`, and `secrets.example.toml` to your secrets folder as `secrets.toml`. Keep both folders outside any synced or shared folder. Never commit either file.
2. Credentials:
   - **Twitch:** an application in the Twitch developer console; then `stream-manager setup twitch-bot` (and `twitch-broadcaster` if the bot isn't the broadcaster) signs in by device code.
   - **YouTube:** a Google Cloud project with the YouTube Data API v3 enabled and an OAuth client of type **TV and Limited Input devices**; put its id and secret under `[youtube]`, then run `stream-manager setup youtube`. Scope: `https://www.googleapis.com/auth/youtube`.
   - **OBS:** the obs-websocket password.
3. Full steps: [docs/setup.md](docs/setup.md). How agents use it: [docs/listener.md](docs/listener.md).

## Privacy

See [PRIVACY.md](PRIVACY.md) and the [terms of service](TERMS.md).

## License

MIT, see [LICENSE](LICENSE).
