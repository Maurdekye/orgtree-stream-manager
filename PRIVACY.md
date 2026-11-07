# Privacy policy: Orgtree stream manager

_Last updated: 2026-11-07_

The Orgtree stream manager is a program that runs on the streamer's own computer. It connects to **the streamer's own** Twitch and YouTube channels during live streams, so the AI agents on the stream can read and answer chat. It is not a hosted service: only the channel owner signs in to it, and there is no server operated by us.

## What it accesses

With the channel owner's permission (on YouTube, Google's "Manage your YouTube account" scope; on Twitch, chat, moderation, bits and subscription scopes), it:

- finds the channel's current live broadcast and its chat;
- reads live-chat messages and stream events on that channel: Super Chats, Super Stickers, memberships and gifted memberships on YouTube; bits, subscriptions and similar events on Twitch;
- posts chat messages as the channel or its bot account, deletes chat messages, and times out or bans chat users, only on the owner's own stream;
- reads whether the stream is live, and ends the owner's own broadcast when the streamer asks;
- reads, from Orgtree running on the same computer, how many agents are busy (a number only), to switch the stream's "sleeping" scene in OBS.

It does not upload, edit or delete videos, and does not read other channels' data, the owner's subscriptions, watch history, email or contacts.

## What it stores, and where

Everything is stored **only on the streamer's computer**:

- **Chat messages:** kept for 7 days, then deleted automatically.
- **Donation messages** (the text attached to a Super Chat, bits or a donation): blanked after 7 days. The amount is kept.
- **Viewer records:** each chatter's platform user or channel ID, when it was first seen, whether it's a subscriber or member, and the donation credit it has built up. They're kept so returning viewers keep their credit.
- **Moderation records:** timeouts and bans it placed, so they can be lifted.
- **Sign-in tokens:** in local files, sent only to the platform that issued them.

There are no analytics, no telemetry, no advertising, and nothing is sold.

## What it sends, and to whom

- **The stream manager itself** sends data only to the platforms' own APIs (Twitch and Google/YouTube) and, on the same computer, to OBS and to Orgtree.
- **The AI agents:** the stream manager passes chat messages (display name and message text) and donation events to the Orgtree agents on the same computer. Those agents use AI model providers' online services (such as Anthropic and OpenAI) to read and write chat replies, so a message's text and display name are sent to those providers while it's being handled. Their own privacy policies apply. Sign-in tokens and keys are never passed to the agents.
- Chat on a public stream is already visible to everyone watching it.

## Revoking access

- Google/YouTube: https://myaccount.google.com/permissions
- Twitch: Settings → Connections, at https://www.twitch.tv/settings/connections

Deleting the stream manager's data folder on the streamer's computer removes everything it stored.

## Google API Services

The stream manager's use of information received from Google APIs follows the [Google API Services User Data Policy](https://developers.google.com/terms/api-services-user-data-policy), including the Limited Use requirements. It uses YouTube API Services: see the [YouTube Terms of Service](https://www.youtube.com/t/terms) and the [Google Privacy Policy](https://policies.google.com/privacy).

## Contact

Questions or requests (including deleting your stored records as a viewer): open an issue at https://github.com/Maurdekye/orgtree-stream-manager/issues.
