use std::{collections::HashMap, env, thread};

use anyhow::Result;
use chrono::{DateTime, Utc};
use rand::{distributions::WeightedIndex, prelude::Distribution, Rng};
use serde_json::json;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

mod common;

// Huge thanks to @Forty-Bot ( https://lobste.rs/u/Forty-Bot ) for coming up with the schema.

fn main() {
    if env::var_os("RUST_LOG").is_none() {
        env::set_var("RUST_LOG", "info,compare-olap-rust=debug");
    }
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Prepare databases
    let sqlite_conn = rusqlite::Connection::open("./normalqlite.db").unwrap();
    sqlite_conn
        .pragma_update(None, "journal_mode", "WAL")
        .unwrap();
    sqlite_conn
        .execute_batch(
            r#"
CREATE TABLE event_types (
  event_id INTEGER PRIMARY KEY,
  event_type TEXT NOT NULL UNIQUE
);

CREATE TABLE form_types (
  form_id INTEGER PRIMARY KEY,
  form_type TEXT NOT NULL UNIQUE
);

CREATE TABLE path_cache (
  path_id INTEGER PRIMARY KEY,
  path TEXT NOT NULL UNIQUE
);

CREATE TABLE user_agents (
  user_agent_id INTEGER PRIMARY KEY,
  user_agent TEXT NOT NULL UNIQUE
);

CREATE TABLE events (
  id INTEGER PRIMARY KEY,
  session_id BLOB NOT NULL,
  page_id BLOB NOT NULL,
  timestamp INT NOT NULL,
  event_id INT NOT NULL REFERENCES event_types (event_id),
  path_id INT REFERENCES path_cache (path_id),
  user_agent_id INT REFERENCES user_agents (user_agent_id),
  text TEXT,
  form_id INT REFERENCES form_types (form_id),
  name TEXT,
  email INT,
  score INT
);

CREATE INDEX events_timestamp ON events(timestamp);
CREATE INDEX events_event_type ON events(event_id, form_id);
CREATE INDEX event_paths ON events(path_id);
"#,
        )
        .unwrap();

    let mut ctx = Ctx::new(sqlite_conn);
    let mut rng = rand::thread_rng();

    // Insert events
    let mut now = Utc::now();
    let max_sessions = 1_000_000;
    tracing::info!("Will insert {max_sessions} sessions");

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
            ctx.persist_event(page_load.clone()).unwrap();

            let mut forms = 0;

            // Up to 20 events per page
            let page_events = rng.gen_range(0..20);
            for _ in 0..page_events {
                let event = generate_event(&ctx, &page_load, timestamp);
                // We only want 1-2 form submissions per page max.
                match event.payload {
                    EventPayload::Feedback { .. } | EventPayload::ContactUs { .. } => {
                        forms += 1;
                        if forms > 1 {
                            continue;
                        }
                    }
                    _ => {}
                }

                ctx.persist_event(event).unwrap();
            }
        }
    }

    tracing::info!("Count SQLite");
    common::exec_sqlite(&ctx.conn, "SELECT count(*) FROM events").unwrap();
    tracing::info!("Done.");
}

#[derive(Clone)]
struct Event {
    id: String,
    session_id: String,
    page_id: String,
    timestamp: DateTime<Utc>,
    payload: EventPayload,
}

#[derive(Clone)]
enum EventPayload {
    PageLoad { path: String, user_agent: String },
    ChatMessage { text: String },
    Feedback { score: i32 },
    ContactUs { name: String, email: String },
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
        payload: EventPayload::PageLoad {
            path: format!("/{path}"),
            user_agent: ctx.random_browser().to_string(),
        },
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
            payload: EventPayload::ChatMessage { text },
        }
    } else if chance < 0.85 {
        let email = format!("{}@{}", ctx.random_word(), ctx.random_word());

        Event {
            id,
            session_id,
            page_id,
            timestamp,
            payload: EventPayload::ContactUs {
                name: ctx.random_word().to_string(),
                email,
            },
        }
    } else {
        let score = rng.gen_range(0..=100);

        Event {
            id,
            session_id,
            page_id,
            timestamp,
            payload: EventPayload::Feedback { score },
        }
    }
}

struct Ctx {
    words: Vec<&'static str>,
    browsers: Vec<&'static str>,
    /// Mapping from event_type to event_id
    event_types: HashMap<String, i32>,
    /// Mapping from user_agent to user_agent_id
    user_agents: HashMap<String, i32>,
    /// Mapping from path to path_id
    paths: HashMap<String, i32>,
    /// Mapping from form_type to form_id
    form_types: HashMap<String, i32>,
    conn: rusqlite::Connection,
}

impl Ctx {
    fn new(conn: rusqlite::Connection) -> Self {
        Self {
            words: WORDS.split("\n").collect(),
            browsers: BROWSERS.split("\n").collect(),
            event_types: Default::default(),
            user_agents: Default::default(),
            paths: Default::default(),
            form_types: Default::default(),
            conn,
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

    fn persist_event(&mut self, e: Event) -> Result<()> {
        let event_id = self.persist_event_type(&e.payload)?;

        match e.payload {
            EventPayload::PageLoad { path, user_agent } => {
                let path_id = self.persist_path(&path)?;
                let ua_id = self.persist_user_agent(&user_agent)?;

                self.conn.execute(
                    r#"
INSERT INTO events (session_id, page_id, timestamp, event_id, path_id, user_agent_id)
  VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                    rusqlite::params![
                        e.session_id,
                        e.page_id,
                        e.timestamp.timestamp(),
                        event_id,
                        path_id,
                        ua_id,
                    ],
                )?;
            }
            EventPayload::ChatMessage { text } => {
                self.conn.execute(
                    r#"
INSERT INTO events (session_id, page_id, timestamp, event_id, text)
  VALUES (?1, ?2, ?3, ?4, ?5)"#,
                    rusqlite::params![
                        e.session_id,
                        e.page_id,
                        e.timestamp.timestamp(),
                        event_id,
                        text,
                    ],
                )?;
            }
            EventPayload::Feedback { score } => {
                let form_id = self.persist_form_type("feedback")?;
                self.conn.execute(
                    r#"
INSERT INTO events (session_id, page_id, timestamp, event_id, form_id, score)
  VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                    rusqlite::params![
                        e.session_id,
                        e.page_id,
                        e.timestamp.timestamp(),
                        event_id,
                        form_id,
                        score,
                    ],
                )?;
            }
            EventPayload::ContactUs { name, email } => {
                let form_id = self.persist_form_type("contact-us")?;
                self.conn.execute(
                    r#"
INSERT INTO events (session_id, page_id, timestamp, event_id, form_id, name, email)
  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
                    rusqlite::params![
                        e.session_id,
                        e.page_id,
                        e.timestamp.timestamp(),
                        event_id,
                        form_id,
                        name,
                        email,
                    ],
                )?;
            }
        }

        Ok(())
    }

    fn persist_event_type(&mut self, p: &EventPayload) -> Result<i32> {
        let event_type = match p {
            EventPayload::PageLoad { .. } => "page_load",
            EventPayload::ChatMessage { .. } => "chat_message",
            EventPayload::Feedback { .. } => "form_submit",
            EventPayload::ContactUs { .. } => "form_submit",
        };

        if let Some(id) = self.event_types.get(event_type) {
            return Ok(*id);
        }

        self.conn.execute(
            "INSERT INTO event_types (event_type) VALUES (?)",
            [event_type],
        )?;
        let id = self.conn.last_insert_rowid() as i32;
        self.event_types.insert(event_type.into(), id);
        Ok(id)
    }

    fn persist_path(&mut self, path: &str) -> Result<i32> {
        if let Some(id) = self.paths.get(path) {
            return Ok(*id);
        }

        self.conn
            .execute("INSERT INTO path_cache (path) VALUES (?)", [path])?;
        let id = self.conn.last_insert_rowid() as i32;
        self.paths.insert(path.into(), id);
        Ok(id)
    }

    fn persist_user_agent(&mut self, ua: &str) -> Result<i32> {
        if let Some(id) = self.user_agents.get(ua) {
            return Ok(*id);
        }

        self.conn
            .execute("INSERT INTO user_agents (user_agent) VALUES (?)", [ua])?;
        let id = self.conn.last_insert_rowid() as i32;
        self.user_agents.insert(ua.into(), id);
        Ok(id)
    }

    fn persist_form_type(&mut self, ft: &str) -> Result<i32> {
        if let Some(id) = self.form_types.get(ft) {
            return Ok(*id);
        }

        self.conn
            .execute("INSERT INTO form_types (form_type) VALUES (?)", [ft])?;
        let id = self.conn.last_insert_rowid() as i32;
        self.form_types.insert(ft.into(), id);
        Ok(id)
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

/**

Queries:

"Average feedback score"

SELECT AVG(score) AS average
  FROM events
  JOIN event_types USING (event_id)
  JOIN form_types USING (form_id)
 WHERE event_type = 'form_submit' AND form_type = 'feedback';

Run Time: real 8.567 user 0.363329 sys 1.573910

"Top pages"

SELECT path, count
  FROM (SELECT path_id, count(*) AS count
          FROM events
          JOIN event_types USING (event_id)
         WHERE event_type = 'page_load'
         GROUP BY path_id
         ORDER BY count DESC
         LIMIT 5
  )
  JOIN path_cache USING (path_id)
 ORDER BY count DESC;

Run Time: real 9.060 user 1.206882 sys 1.560014

 */
struct DummyOtherwiseRustComplainsAboutTheComment;
