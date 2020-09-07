#![feature(map_first_last)]

use tide::{sse, Response, StatusCode, Request};
use xactor::{Actor, Handler, Context, Message, Addr};
use crate::InnerMessage::{Clean, Next};
use sqlx::{SqlitePool, Sqlite};
use structopt::*;
use sqlx::prelude::SqliteQueryAs;
use std::collections::BTreeSet;

#[global_allocator]
static MALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

struct HouseKeeper {
    registry: hashbrown::HashMap<uuid::Uuid, xactor::Addr<SSEActor>>,
    current_key: std::time::SystemTime,
    valid: std::collections::BTreeSet<std::time::SystemTime>,
    pool: SqlitePool,
    encryptor: botan::Encryptor,
    decryptor: botan::Decryptor,
    finished: bool,
}

struct SSEActor {
    tag: uuid::Uuid,
    sender: sse::Sender,
    keeper: xactor::Addr<HouseKeeper>,
}


#[xactor::message(result = "()")]
#[derive(Clone)]
enum InnerMessage {
    Register(uuid::Uuid, xactor::Addr<SSEActor>),
    UnRegister(uuid::Uuid),
    Next,
    Clean,
}

#[xactor::message(result = "String")]
struct GetCurrentQR;

#[xactor::message(result = "()")]
#[derive(Clone)]
enum SendEvent {
    ChangeQR(String),
    UpdatePoolSize(usize),
    DrawResult(DrawResult),
}

#[derive(Clone)]
#[derive(serde::Serialize, serde::Deserialize)]
struct DrawResult {
    first_class: Vec<String>,
    second_class: Vec<String>,
    third_class: Vec<String>,
}

#[async_trait::async_trait]
impl Handler<GetCurrentQR> for HouseKeeper {
    async fn handle(&mut self, _: &mut Context<Self>, _: GetCurrentQR) -> String {
        let buffer = simd_json::to_vec(&self.current_key).unwrap();
        let random = botan::RandomNumberGenerator::new_system().unwrap();
        let encrypted = self.encryptor.encrypt(&buffer, &random).unwrap();
        radix64::URL_SAFE.encode(&encrypted)
    }
}

unsafe impl Send for HouseKeeper {}

unsafe impl Sync for HouseKeeper {}

#[async_trait::async_trait]
impl Handler<InnerMessage> for HouseKeeper {
    async fn handle(&mut self, _: &mut Context<Self>, msg: InnerMessage) -> () {
        match msg {
            InnerMessage::Register(uuid, addr) => {
                self.registry.insert(uuid, addr.clone());
                if self.finished {
                    if let Err(e) = self.get_results().await
                        .and_then(|x| addr.send(x).map_err(Into::into)) {
                        log::error!("failed to send results: {}", e)
                    }
                }
            }
            InnerMessage::UnRegister(uuid) => {
                self.registry.remove(&uuid);
            }
            InnerMessage::Next => {
                if !self.finished {
                    self.current_key = std::time::SystemTime::now();
                    self.valid.insert(self.current_key.clone());
                    let buffer = simd_json::to_vec(&self.current_key).unwrap();
                    let random = botan::RandomNumberGenerator::new_system().unwrap();
                    let encrypted = self.encryptor.encrypt(&buffer, &random).unwrap();
                    let msg = radix64::URL_SAFE.encode(&encrypted);
                    for (_, actor) in self.registry.iter() {
                        if let Err(e) = actor.send(SendEvent::ChangeQR(msg.clone())) {
                            log::error!("failed to communicate with actor {}: {}", actor.actor_id(), e)
                        }
                    }
                }
            }
            InnerMessage::Clean => {
                let now = std::time::SystemTime::now();
                while let Some(x) = self.valid.first() {
                    if now.duration_since(x.clone()).unwrap().as_secs() >= 60 {
                        self.valid.pop_first();
                    } else {
                        break;
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<SendEvent> for SSEActor {
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: SendEvent) -> () {
        let sending = match msg {
            SendEvent::ChangeQR(msg) => self.sender.send("change-qr", msg, None),
            SendEvent::UpdatePoolSize(size) => self.sender.send("update-size", size.to_string(), None),
            SendEvent::DrawResult(r) => {
                self.sender.send("draw_result", simd_json::to_string(&r).unwrap(), None)
            }
        };
        match sending.await {
            Ok(_) => log::debug!("actor {} send data successfully", ctx.actor_id()),
            Err(e) => {
                log::error!("actor {} failed to send data: {}", ctx.actor_id(), e);
                if let Err(e) = self.keeper.send(InnerMessage::UnRegister(self.tag.clone())) {
                    log::error!("unable to unregister myself (id={}): {}", ctx.actor_id(), e);
                }
                ctx.stop(None);
            }
        }
    }
}

impl SSEActor {
    async fn start_new(keeper: &Addr<HouseKeeper>, sender: sse::Sender) -> anyhow::Result<Addr<Self>> {
        let tag = uuid::Uuid::new_v4();
        let current = keeper.call(GetCurrentQR).await?;
        sender.send("change-qr", current, None).await?;
        let actor = SSEActor {
            tag: tag.clone(),
            sender,
            keeper: keeper.clone(),
        };
        let addr = actor.start().await?;
        keeper.send(InnerMessage::Register(tag.clone(), addr.clone()))?;
        Ok(addr)
    }
}

#[async_trait::async_trait]
impl Actor for HouseKeeper {
    async fn started(&mut self, ctx: &mut Context<Self>) -> xactor::Result<()> {
        ctx.send_interval(Clean, std::time::Duration::from_secs(15));
        ctx.send_interval(Next, std::time::Duration::from_secs(30));
        Ok(())
    }
}

impl Actor for SSEActor {}

#[xactor::message(result = "SubmitReply")]
#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Submit {
    student_id: String,
    validation: String,
}


#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct SubmitReply {
    status: bool,
    msg: String,
}

#[async_trait::async_trait]
impl Handler<Submit> for HouseKeeper {
    async fn handle(&mut self, _: &mut Context<Self>, msg: Submit) -> <Submit as Message>::Result {
        let validation = radix64::URL_SAFE.decode(&msg.validation)
            .map_err(Into::<anyhow::Error>::into)
            .and_then(|x| {
                self.decryptor.decrypt(x.as_slice())
                    .map_err(|e| anyhow::anyhow!("{:?}", e))
            })
            .and_then(|mut x| {
                simd_json::from_slice::<std::time::SystemTime>(&mut x)
                    .map_err(Into::<anyhow::Error>::into)
            });
        if validation.is_err() {
            return SubmitReply {
                status: false,
                msg: validation.unwrap_err().to_string(),
            };
        }
        let validation = validation.unwrap();
        if self.valid.contains(&validation) {
            match sqlx::query("INSERT INTO record (student_id) VALUES (?)")
                .bind(msg.student_id)
                .execute(&self.pool)
                .await {
                Ok(_) => {
                    if let Ok((size, )) = sqlx::query_as::<Sqlite, (i32, )>("SELECT COUNT(id) FROM record")
                        .fetch_one(&self.pool).await {
                        for (_, j) in self.registry.iter() {
                            j.send(SendEvent::UpdatePoolSize(size as usize)).unwrap();
                        }
                    }
                    SubmitReply {
                        status: true,
                        msg: String::from("recorded"),
                    }
                }
                Err(e) => SubmitReply {
                    status: false,
                    msg: e.to_string(),
                }
            }
        } else {
            SubmitReply {
                status: false,
                msg: String::from("invalid uuid"),
            }
        }
    }
}

#[derive(StructOpt, Debug)]
struct Opt {
    #[structopt(short, long, help = "Database url")]
    database: String,
}

#[xactor::message(result = "anyhow::Result<()>")]
#[derive(serde::Serialize, serde::Deserialize)]
struct Draw {
    first_class: usize,
    second_class: usize,
    third_class: usize,
}


static UPDATE_SQL: &str =
    "UPDATE record SET grade = ? WHERE id IN (SELECT id FROM record WHERE grade = 0 ORDER BY RANDOM() LIMIT ?)";

impl HouseKeeper {
    async fn get_results(&self) -> anyhow::Result<SendEvent> {
        let first_class: Vec<String> = sqlx::query_as("SELECT student_id FROM record WHERE grade = 1")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|x: (String, )| x.0)
            .collect();
        let second_class: Vec<String> = sqlx::query_as("SELECT student_id FROM record WHERE grade = 2")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|x: (String, )| x.0)
            .collect();
        let third_class: Vec<String> = sqlx::query_as("SELECT student_id FROM record WHERE grade = 3")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|x: (String, )| x.0)
            .collect();
        Ok(SendEvent::DrawResult(DrawResult {
            first_class,
            second_class,
            third_class,
        }))
    }
    async fn send_results(&self) -> anyhow::Result<()> {
        let result = self.get_results().await?;
        for (_, actor) in self.registry.iter() {
            if let Err(e) = actor.send(result.clone()) {
                log::error!("failed to communicate with actor {}: {}", actor.actor_id(), e);
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<Draw> for HouseKeeper {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: Draw) -> anyhow::Result<()> {
        if !self.finished {
            sqlx::query(UPDATE_SQL)
                .bind(1)
                .bind(msg.first_class as i32)
                .execute(&self.pool)
                .await?;
            sqlx::query(UPDATE_SQL)
                .bind(2)
                .bind(msg.second_class as i32)
                .execute(&self.pool)
                .await?;
            sqlx::query(UPDATE_SQL)
                .bind(3)
                .bind(msg.third_class as i32)
                .execute(&self.pool)
                .await?;
        }
        self.send_results().await?;
        self.finished = true;
        Ok(())
    }
}

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    tide::log::start();
    let opt = Opt::from_args();
    let pool = sqlx::sqlite::SqlitePool::new(&opt.database)
        .await?;
    sqlx::query(r#"
        CREATE TABLE IF NOT EXISTS 'record' (
            id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL,
            time DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
            student_id TEXT NOT NULL UNIQUE,
            grade INTEGER NOT NULL DEFAULT 0
        )"#).execute(&pool).await?;
    log::info!("SQL started");
    let (finished, ) = sqlx::query_as("SELECT EXISTS(SELECT id FROM record WHERE grade != 0)")
        .fetch_one(&pool)
        .await?;
    if finished {
        log::info!("already finished");
    }
    let random = botan::RandomNumberGenerator::new_system().unwrap();
    let key = botan::Privkey::create("RSA", "1024", &random).unwrap();
    let pkey = key.pubkey().unwrap();
    log::debug!("RSA initialized with {}", key.pem_encode().unwrap());
    let decryptor = botan::Decryptor::new(&key, "OAEP(MD5)").unwrap();
    let encryptor = botan::Encryptor::new(&pkey, "OAEP(MD5)").unwrap();
    let current_key = std::time::SystemTime::now();
    let mut valid = BTreeSet::new();
    valid.insert(current_key.clone());
    let keeper = HouseKeeper {
        valid,
        registry: Default::default(),
        current_key,
        decryptor,
        encryptor,
        pool,
        finished,
    }.start().await?;
    let mut app = tide::Server::with_state(keeper);
    app.with(tide_compress::CompressMiddleware::new());
    app.at("/sse").get(sse::endpoint(|req, sender| {
        async move {
            match SSEActor::start_new(req.state(), sender).await {
                Ok(addr) => log::info!("actor started at: {}", addr.actor_id()),
                Err(e) => log::error!("failed to start actor: {}", e)
            }
            Ok(())
        }
    }));
    app.at("/submit").post(|mut req: Request<Addr<HouseKeeper>>, | async move {
        let mut bytes = req.body_bytes().await?;
        let json: Submit = simd_json::from_slice(&mut bytes)?;
        let reply: SubmitReply = req.state().call(json).await?;
        let body = simd_json::to_string(&reply)?;
        Ok(Response::builder(StatusCode::Ok)
            .content_type(tide::http::mime::JSON)
            .body(body))
    });
    app.at("/draw").post(|mut req: Request<Addr<HouseKeeper>>, | async move {
        let mut bytes: Vec<u8> = req.body_bytes().await?;
        let json: Draw = simd_json::from_slice(&mut bytes)?;
        req.state().call(json).await??;
        Ok(Response::new(StatusCode::Ok))
    });
    app.listen("0.0.0.0:8080").await?;
    Ok(())
}
