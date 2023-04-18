use std::{env, thread};

use chrono::{DateTime, Utc};
use rand::{distributions::WeightedIndex, prelude::Distribution, Rng};
use serde_json::json;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

mod common;

fn main() {
    if env::var_os("RUST_LOG").is_none() {
        env::set_var("RUST_LOG", "info,compare-olap-rust=debug");
    }
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Prepare databases
    let sqlite_conn = rusqlite::Connection::open("./eventsqlite.db").unwrap();
    sqlite_conn
        .pragma_update(None, "journal_mode", "WAL")
        .unwrap();
    sqlite_conn
        .execute(
            r#"
CREATE TABLE events (
  id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  page_id TEXT NOT NULL,
  timestamp TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload TEXT
);
CREATE INDEX events_timestamp ON events(timestamp);
CREATE INDEX events_event_type ON events(event_type);
"#,
            [],
        )
        .unwrap();

    let duck_conn = duckdb::Connection::open("./eventsduck.db").unwrap();
    duck_conn
        .execute(
            r#"
CREATE TABLE events (
  id VARCHAR NOT NULL,
  session_id VARCHAR NOT NULL,
  page_id VARCHAR NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  event_type VARCHAR NOT NULL,
  payload JSON
);
"#,
            [],
        )
        .unwrap();

    let duck_typed_conn = duckdb::Connection::open("./eventsduck-typed.db").unwrap();
    duck_typed_conn
        .execute(
            r#"
CREATE TABLE events (
  id VARCHAR NOT NULL,
  session_id VARCHAR NOT NULL,
  page_id VARCHAR NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  event_type VARCHAR NOT NULL,
  payload STRUCT(
    path VARCHAR,
    user_agent VARCHAR,
    text VARCHAR,
    form_type VARCHAR,
    fields STRUCT(name VARCHAR, value VARCHAR)[]
  )
);
"#,
            [],
        )
        .unwrap();

    let ctx = Ctx::new();
    let mut rng = rand::thread_rng();

    // Insert events
    let mut now = Utc::now();
    let max_sessions = 100_000;
    tracing::info!("Will insert {max_sessions} sessions");

    let (sqlite_tx, sqlite_rx) = std::sync::mpsc::sync_channel::<Event>(1);
    let (duck_tx, duck_rx) = std::sync::mpsc::sync_channel::<Event>(1);
    let (duck_typed_tx, duck_typed_rx) = std::sync::mpsc::sync_channel::<Event>(1);

    let sqlite_handle = thread::spawn(move || {
        tracing::info!("SQLite worker running");

        while let Ok(e) = sqlite_rx.recv() {
            let payload = serde_json::to_string(&e.payload).unwrap();
            sqlite_conn
                .execute(
                    r#"
INSERT INTO events (id, session_id, page_id, timestamp, event_type, payload)
  VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                    rusqlite::params![
                        e.id,
                        e.session_id,
                        e.page_id,
                        e.timestamp,
                        e.r#type,
                        payload,
                    ],
                )
                .unwrap();
        }

        tracing::info!("Count SQLite");
        common::exec_sqlite(&sqlite_conn, "SELECT count(*) FROM events").unwrap();
    });

    let duck_handle = thread::spawn(move || {
        tracing::info!("DuckDB worker running");

        while let Ok(e) = duck_rx.recv() {
            let payload = serde_json::to_string(&e.payload).unwrap();
            duck_conn
                .execute(
                    r#"
INSERT INTO events (id, session_id, page_id, timestamp, event_type, payload)
  VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                    duckdb::params![
                        e.id,
                        e.session_id,
                        e.page_id,
                        e.timestamp,
                        e.r#type,
                        payload,
                    ],
                )
                .unwrap();
        }

        tracing::info!("Count DuckDB");
        common::exec_duck(&duck_conn, "SELECT count(*) FROM events", vec!["count"]).unwrap();
    });

    let duck_typed_handle = thread::spawn(move || {
        tracing::info!("DuckDB-typed worker running");

        while let Ok(e) = duck_typed_rx.recv() {
            let path = e.payload.get("path").and_then(|v| v.as_str());
            let user_agent = e.payload.get("user_agent").and_then(|v| v.as_str());
            let text = e.payload.get("text").and_then(|v| v.as_str());
            let form_type = e.payload.get("form_type").and_then(|v| v.as_str());
            let fields = e
                .payload
                .get("fields")
                .map(|v| serde_json::to_string(&v).unwrap().replace('"', "'"))
                .unwrap_or("null".into());

            // Sample query
            // INSERT INTO EVENTS (id, timestamp, event_type, payload) VALUES ('123', '2023-04-16 23:05:40', 'page_load', { 'path': '/', 'user_agent': null, 'text': null, 'form_type': null, 'fields': [{'name': 'Score', 'value': '70'}] });
            duck_typed_conn
            .execute(
                &format!(r#"
INSERT INTO events (id, session_id, page_id, timestamp, event_type, payload)
  VALUES (?1, ?2, ?3, ?4, ?5, {{ 'path': ?6, 'user_agent': ?7, 'text': ?8, 'form_type': ?9, 'fields': {fields} }})"#),
                duckdb::params![
                    e.id,
                    e.session_id,
                    e.page_id,
                    e.timestamp,
                    e.r#type,
                    path,
                    user_agent,
                    text,
                    form_type,
                ],
            ).unwrap();
        }

        tracing::info!("Count DuckDB Typed");
        common::exec_duck_typed(
            &duck_typed_conn,
            "SELECT count(*) FROM events",
            vec!["count"],
        )
        .unwrap();
    });

    for i in 0..max_sessions {
        let timestamp = now.clone();
        let secs: i8 = rand::random();
        now += chrono::Duration::seconds(secs.abs() as i64);

        if i % 10000 == 0 {
            tracing::info!("#{i}/{max_sessions}: Inserting session");
        }

        // Chances that single session has:
        // 40% to have 1  page load
        // 30% to have 2  page loads
        // 20% to have 4  page loads
        // 8%  to have 8  page loads
        // 2%  to have 12 page loads
        let page_load_choices = [1, 2, 4, 8, 12];
        let page_load_weights = [40 as usize, 30, 20, 8, 2];
        let page_load_dist = WeightedIndex::new(&page_load_weights[..]).unwrap();
        let page_loads = page_load_choices[page_load_dist.sample(&mut rng)];

        let session_id = Uuid::new_v4().to_string();

        for _ in 0..page_loads {
            let page_load = generate_page_load(&ctx, &session_id, timestamp);
            sqlite_tx.send(page_load.clone()).unwrap();
            duck_tx.send(page_load.clone()).unwrap();
            duck_typed_tx.send(page_load.clone()).unwrap();

            let mut forms = 0;

            // Up to 20 events per page
            let page_events = rng.gen_range(0..20);
            for _ in 0..page_events {
                let event = generate_event(&ctx, &page_load, timestamp);
                // We only want 1-2 form submissions per page max.
                if event.r#type == "form_submit" {
                    forms += 1;
                    if forms > 1 {
                        continue;
                    }
                }

                sqlite_tx.send(event.clone()).unwrap();
                duck_tx.send(event.clone()).unwrap();
                duck_typed_tx.send(event).unwrap();
            }
        }
    }

    tracing::info!("Done sending events.");

    drop(sqlite_tx);
    drop(duck_tx);
    drop(duck_typed_tx);

    sqlite_handle.join().unwrap();
    duck_handle.join().unwrap();
    duck_typed_handle.join().unwrap();

    tracing::info!("Done.");
}

#[derive(Clone)]
struct Event {
    id: String,
    session_id: String,
    page_id: String,
    timestamp: DateTime<Utc>,
    r#type: String,
    payload: serde_json::Value,
}

fn generate_page_load(ctx: &Ctx, session_id: &str, timestamp: DateTime<Utc>) -> Event {
    let id = Uuid::new_v4().to_string();
    let path = ctx.random_path();
    let page_id = Uuid::new_v4().to_string();

    Event {
        id,
        session_id: session_id.into(),
        page_id,
        timestamp,
        r#type: "page_load".into(),
        payload: json!({
            "path": format!("/{path}"),
            "user_agent": ctx.random_browser(),
        }),
    }
}

fn generate_event(ctx: &Ctx, page: &Event, timestamp: DateTime<Utc>) -> Event {
    let mut rng = rand::thread_rng();
    let id = Uuid::new_v4().to_string();
    let session_id = page.session_id.to_string();
    let page_id = page.page_id.to_string();

    // A random number [0, 1)
    let chance: f32 = rand::random();
    if chance < 0.7 {
        let text = ctx.random_text();

        Event {
            id,
            session_id,
            page_id,
            timestamp,
            r#type: "chat_message".into(),
            payload: json!({
                "text": text,
            }),
        }
    } else if chance < 0.85 {
        let email = format!("{}@{}", ctx.random_word(), ctx.random_word());

        Event {
            id,
            session_id,
            page_id,
            timestamp,
            r#type: "form_submit".into(),
            payload: json!({
                "form_type": "contact-us",
                "fields": [{
                    "name": "name",
                    "value": ctx.random_word(),
                }, {
                    "name": "email",
                    "value": email,
                }],
            }),
        }
    } else {
        let score = rng.gen_range(0..=100);

        Event {
            id,
            session_id,
            page_id,
            timestamp,
            r#type: "form_submit".into(),
            payload: json!({
                "form_type": "feedback",
                "fields": [{
                    "name": "score",
                    "value": format!("{score}"),
                }],
            }),
        }
    }
}

struct Ctx {
    words: Vec<&'static str>,
    browsers: Vec<&'static str>,
}

impl Ctx {
    fn new() -> Self {
        Self {
            words: WORDS.split("\n").collect(),
            browsers: BROWSERS.split("\n").collect(),
        }
    }

    fn random_path(&self) -> &'static str {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..40);
        self.words[index]
    }

    fn random_word(&self) -> &'static str {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..self.words.len());
        self.words[index]
    }

    fn random_text(&self) -> String {
        let mut rng = rand::thread_rng();
        let words = rng.gen_range(1..30);
        (0..words)
            .map(|_| self.random_word())
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn random_browser(&self) -> &'static str {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..self.browsers.len());
        self.browsers[index]
    }
}

const BROWSERS: &'static str = r#"
Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15
Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0
Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0
Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36
Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36 OPR/38.0.2220.41
Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59
Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1
Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/111.0
Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)
curl/7.64.1"#;

// 200 most common words
const WORDS: &'static str = r#"water
away
good
want
over
how
did
man
going
where
would
or
took
school
think
home
who
didn’t
ran
know
bear
can’t
again
cat
long
things
new
after
wanted
eat
everyone
our
two
has
yes
play
take
thought
dog
well
find
more
I’ll
round
tree
magic
shouted
us
other
food
fox
through
way
been
stop
must
red
door
right
sea
these
began
boy
animals
never
next
first
work
lots
need
that’s
baby
fish
gave
mouse
something
bed
may
still
found
live
say
soon
night
narrator
small
car
couldn’t
three
head
king
town
I’ve
around
every
garden
fast
only
many
laughed
5let’s
much
suddenly
told
another
great
why
cried
keep
room
last
jumped
because
even
am
before
gran
clothes
tell
key
fun
place
mother
sat
boat
window
sleep
feet
morning
queen
each
book
its
green
different
let
girl
which
inside
run
any
under
hat
snow
air
trees
bad
tea
top
eyes
fell
friends
box
dark
grandad
there’s
looking
end
than
best
better
hot
sun
across
gone
hard
floppy
really
wind
wish
eggs
once
please
thing
stopped
ever
miss
most
cold
park
lived
birds
duck
horse
rabbit
white
coming
he’s
river
liked
giant
looks
use
along
plants
dragon
pulled
we’re
fly
grow"#;
