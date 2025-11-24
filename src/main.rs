use axum::{
    extract::{ws::{Message, WebSocketUpgrade}, State, Path},
    routing::{get, post, delete},
    Json, Router,
};
use tower_http::{
    cors::CorsLayer,
    services::ServeDir,
};
use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use ndarray::Array2;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Pool, Sqlite,
};
use std::{
    collections::HashMap,
    io::Cursor,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
    fs,
    str::FromStr,
};
use tokio::sync::broadcast;
use zeromq::{Socket, SocketRecv};
use image::{ImageBuffer, Rgb};

// --- CONFIGURACIÓN ---
#[derive(Deserialize, Serialize, Clone, Debug)]
struct Config {
    turbine: TurbineConfig,
    limits: LimitsConfig,
    network: NetworkConfig,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct TurbineConfig {
    park_id: u32,
    turbine_number: u32,
    model: String,
    #[serde(rename = "type")]
    turbine_type: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct LimitsConfig {
    max_temp_warning: f32,
    notification_cooldown_min: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct NetworkConfig {
    zmq_endpoint: String,
    mqtt_broker: String,
    mqtt_port: u16,
    http_port: u16,
    db_url: String,
}

// --- ESTADO GLOBAL ---
struct AppState {
    config: Config,
    current_gsu: Arc<Mutex<u8>>,
    mqtt_client: AsyncClient,
    last_notifications: Arc<Mutex<HashMap<u8, Instant>>>,
    tx_binary_stream: broadcast::Sender<Vec<u8>>,
    db_pool: Pool<Sqlite>,
    // Health Check & Uptime
    last_heartbeat_mqtt: Arc<Mutex<Instant>>,
    last_heartbeat_zmq: Arc<Mutex<Instant>>,
    start_time: Instant,
}

// --- MODELO DB ---
#[derive(sqlx::FromRow, Serialize)]
struct AlertRecord {
    id: i64,
    timestamp: chrono::DateTime<chrono::Utc>,
    gsu_id: u8,
    max_temp: f32,
    full_image_path: String,
    hotspot_image_path: String,
}

// --- ESTRUCTURA HEALTH CHECK ---
#[derive(Serialize)]
struct HealthStatus {
    mqtt_status: String,
    zeromq_status: String,
    uptime_seconds: u64,
    timestamp: String,
}

// --- PARSEO HEADER ---
fn parse_header(bytes: &[u8]) -> anyhow::Result<(u64, i32, i32, i32)> {
    let mut rdr = Cursor::new(bytes);
    let timestamp = rdr.read_u64::<LittleEndian>()?;
    let rows = rdr.read_i32::<LittleEndian>()?;
    let cols = rdr.read_i32::<LittleEndian>()?;
    let type_ = rdr.read_i32::<LittleEndian>()?;
    Ok((timestamp, rows, cols, type_))
}

// --- PROCESAMIENTO DE IMÁGENES ---

fn generate_complete_heatmap(matrix: &Array2<f32>, min: f32, max: f32) -> Vec<u8> {
    let (rows, cols) = matrix.dim();
    let mut img = ImageBuffer::new(cols as u32, rows as u32);
    let range = (max - min).max(1.0);

    for (y, row) in matrix.outer_iter().enumerate() {
        for (x, &temp) in row.iter().enumerate() {
            let norm = (temp - min) / range;
            let r = (norm * 255.0) as u8;
            let b = ((1.0 - norm) * 255.0) as u8;
            // Verde suave en rango medio
            let g = if norm > 0.3 && norm < 0.7 { 100 } else { 0 }; 
            img.put_pixel(x as u32, y as u32, Rgb([r, g, b]));
        }
    }
    encode_png(img)
}

fn generate_hotspot_visual(matrix: &Array2<f32>, min: f32, max: f32, threshold: f32) -> Vec<u8> {
    let (rows, cols) = matrix.dim();
    let mut img = ImageBuffer::new(cols as u32, rows as u32);
    let range = (max - min).max(1.0);

    for (y, row) in matrix.outer_iter().enumerate() {
        for (x, &temp) in row.iter().enumerate() {
            if temp >= threshold {
                // ALERTA: Rojo Puro
                img.put_pixel(x as u32, y as u32, Rgb([255, 0, 0])); 
            } else {
                // FONDO: Escala de grises
                let norm = (temp - min) / range;
                let gray = (norm * 150.0) as u8; 
                img.put_pixel(x as u32, y as u32, Rgb([gray, gray, gray]));
            }
        }
    }
    encode_png(img)
}

fn encode_png(img: ImageBuffer<Rgb<u8>, Vec<u8>>) -> Vec<u8> {
    let mut buffer = Cursor::new(Vec::new());
    img.write_to(&mut buffer, image::ImageOutputFormat::Png).unwrap();
    buffer.into_inner()
}

// --- MAIN ---
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let start_time = Instant::now();

    // 1. Crear estructura de carpetas
    fs::create_dir_all("data/alerts/images/complete")?;
    fs::create_dir_all("data/alerts/images/puntos_calientes")?;
    
    // 2. Cargar Configuración
    let config_content = std::fs::read_to_string("config.toml").expect("Falta config.toml");
    let config: Config = toml::from_str(&config_content)?;

    // 3. Conexión DB (Crear si no existe)
    let db_options = SqliteConnectOptions::from_str(&config.network.db_url)?
        .create_if_missing(true);

    let db_pool = SqlitePoolOptions::new()
        .connect_with(db_options)
        .await
        .expect("Error DB");
    
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME NOT NULL,
            gsu_id INTEGER NOT NULL,
            max_temp REAL NOT NULL,
            full_image_path TEXT NOT NULL,
            hotspot_image_path TEXT NOT NULL
        )"
    ).execute(&db_pool).await?;

    // 4. Cliente MQTT
    let mut mqttoptions = MqttOptions::new(
        format!("core_{}", config.turbine.turbine_number),
        &config.network.mqtt_broker, config.network.mqtt_port,
    );
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    let (mqtt_client, mut mqtt_eventloop) = AsyncClient::new(mqttoptions, 10);

    // 5. Estado Compartido
    let current_gsu = Arc::new(Mutex::new(0));
    let last_notifications = Arc::new(Mutex::new(HashMap::new()));
    let (tx_binary_stream, _) = broadcast::channel(10);
    
    let last_heartbeat_mqtt = Arc::new(Mutex::new(Instant::now()));
    let last_heartbeat_zmq = Arc::new(Mutex::new(Instant::now()));

    let state = Arc::new(AppState {
        config: config.clone(),
        current_gsu: current_gsu.clone(),
        mqtt_client: mqtt_client.clone(),
        last_notifications: last_notifications.clone(),
        tx_binary_stream: tx_binary_stream.clone(),
        db_pool: db_pool.clone(),
        last_heartbeat_mqtt: last_heartbeat_mqtt.clone(),
        last_heartbeat_zmq: last_heartbeat_zmq.clone(),
        start_time,
    });

    // 6. Tarea MQTT (Listener + Heartbeat)
    let gsu_clone = current_gsu.clone();
    let hb_mqtt_clone = state.last_heartbeat_mqtt.clone();
    tokio::spawn(async move {
        mqtt_client.subscribe("gsu/data/status", QoS::AtMostOnce).await.unwrap();
        loop {
            if let Ok(event) = mqtt_eventloop.poll().await {
                *hb_mqtt_clone.lock().unwrap() = Instant::now();
                if let rumqttc::Event::Incoming(rumqttc::Packet::Publish(p)) = event {
                    if p.topic == "gsu/data/status" {
                        let val_str = String::from_utf8_lossy(&p.payload);
                        if let Ok(val) = val_str.trim().parse::<u8>() {
                            *gsu_clone.lock().unwrap() = val;
                        }
                    }
                }
            }
        }
    });

    // 7. Tarea ZMQ (Ingestión + Procesamiento + Alertas)
    let zmq_endpoint = config.network.zmq_endpoint.clone();
    let state_zmq = state.clone();

    tokio::spawn(async move {
        let mut socket = zeromq::SubSocket::new();
        socket.connect(&zmq_endpoint).await.expect("Error ZMQ");
        socket.subscribe("").await.expect("Error Sub ZMQ");

        loop {
            match socket.recv().await {
                Ok(msg) => {
                    *state_zmq.last_heartbeat_zmq.lock().unwrap() = Instant::now();

                    if msg.len() < 2 { continue; }
                    let header_bytes = msg.get(0).unwrap();
                    let payload_bytes = msg.get(1).unwrap();

                    if let Ok((ts, rows, cols, _)) = parse_header(&header_bytes) {
                        let float_count = (rows * cols) as usize;
                        if payload_bytes.len() == float_count * 4 {
                            let mut floats = vec![0.0; float_count];
                            let mut rdr = Cursor::new(&payload_bytes);
                            for i in 0..float_count {
                                floats[i] = rdr.read_f32::<LittleEndian>().unwrap_or(0.0);
                            }
                            let matrix = Array2::from_shape_vec((rows as usize, cols as usize), floats.clone()).unwrap();
                            
                            let max = *matrix.iter().reduce(|a, b| if a > b { a } else { b }).unwrap_or(&0.0);
                            let min = *matrix.iter().reduce(|a, b| if a < b { a } else { b }).unwrap_or(&0.0);
                            let sum: f32 = matrix.sum();
                            let avg = sum / float_count as f32;
                            let gsu_id = *state_zmq.current_gsu.lock().unwrap();

                            // Lógica de Alerta
                            let alert_threshold = state_zmq.config.limits.max_temp_warning;
                            let should_notify = if max > alert_threshold && gsu_id != 0 {
                                let mut notifs = state_zmq.last_notifications.lock().unwrap();
                                let allow = match notifs.get(&gsu_id) {
                                    Some(last) => last.elapsed() > Duration::from_secs(state_zmq.config.limits.notification_cooldown_min * 60),
                                    None => true,
                                };
                                if allow { notifs.insert(gsu_id, Instant::now()); }
                                allow
                            } else { false };

                            if should_notify {
                                println!("🚨 ALERTA GSU {}: {}°C", gsu_id, max);
                                let filename = format!("alert_{}_{}.png", ts, gsu_id);
                                
                                // Generar imágenes
                                let img_full = generate_complete_heatmap(&matrix, min, max);
                                let path_full = format!("data/alerts/images/complete/{}", filename);
                                let _ = fs::write(&path_full, &img_full);

                                let img_hot = generate_hotspot_visual(&matrix, min, max, alert_threshold);
                                let path_hot = format!("data/alerts/images/puntos_calientes/hotspot_{}", filename);
                                let _ = fs::write(&path_hot, &img_hot);

                                // Guardar en BD
                                let _ = sqlx::query(
                                    "INSERT INTO alerts (timestamp, gsu_id, max_temp, full_image_path, hotspot_image_path) VALUES (datetime('now'), ?, ?, ?, ?)"
                                )
                                .bind(gsu_id as i32)
                                .bind(max)
                                .bind(&path_full)
                                .bind(&path_hot)
                                .execute(&state_zmq.db_pool).await;
                            }

                            // Stream binario (WebSockets)
                            let mut packet = Vec::with_capacity(22 + payload_bytes.len());
                            let _ = packet.write_u8(0xAF);
                            let _ = packet.write_u8(gsu_id);
                            let _ = packet.write_u64::<LittleEndian>(ts);
                            let _ = packet.write_f32::<LittleEndian>(max);
                            let _ = packet.write_f32::<LittleEndian>(avg);
                            let _ = packet.write_u32::<LittleEndian>(payload_bytes.len() as u32);
                            packet.extend_from_slice(&payload_bytes); 
                            let _ = state_zmq.tx_binary_stream.send(packet);
                        }
                    }
                },
                Err(_) => {}
            }
        }
    });

    // 8. Router HTTP
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/api/control", post(control_handler))
        .route("/api/health", get(health_handler))
        // Endpoints de Alertas (GET y DELETE All)
        .route("/api/alerts", get(get_alerts_handler).delete(delete_all_alerts_handler))
        // Endpoint de borrado individual
        .route("/api/alerts/:id", delete(delete_alert_handler))
        // Servir Archivos Estáticos
        .nest_service("/data", ServeDir::new("data"))
        .layer(CorsLayer::permissive())
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", config.network.http_port)).await?;
    println!("🟢 GSU Core v2.2 (Full) corriendo en puerto {}", config.network.http_port);
    axum::serve(listener, app).await?;

    Ok(())
}

// --- HANDLERS ---

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> axum::response::Response {
    ws.on_upgrade(|mut socket| async move {
        let mut rx = state.tx_binary_stream.subscribe();
        while let Ok(packet) = rx.recv().await {
            if socket.send(Message::Binary(packet)).await.is_err() { break; }
        }
    })
}

#[derive(Deserialize)]
struct ControlCmd { mode: Option<String>, move_dir: Option<String> }
async fn control_handler(State(state): State<Arc<AppState>>, Json(payload): Json<ControlCmd>) -> Json<serde_json::Value> {
    if let Some(m) = payload.mode { let _ = state.mqtt_client.publish("gsu/control/mode", QoS::AtLeastOnce, false, m.as_bytes()).await; }
    if let Some(d) = payload.move_dir { let _ = state.mqtt_client.publish("gsu/control/manual", QoS::AtLeastOnce, false, d.as_bytes()).await; }
    Json(serde_json::json!({"status": "ok"}))
}

async fn get_alerts_handler(State(state): State<Arc<AppState>>) -> Json<Vec<AlertRecord>> {
    let alerts = sqlx::query_as::<_, AlertRecord>("SELECT id, timestamp, gsu_id, max_temp, full_image_path, hotspot_image_path FROM alerts ORDER BY timestamp DESC LIMIT 50")
        .fetch_all(&state.db_pool)
        .await
        .unwrap_or(vec![]);
    Json(alerts)
}

async fn health_handler(State(state): State<Arc<AppState>>) -> Json<HealthStatus> {
    let timeout = Duration::from_secs(10);
    let now = Instant::now();

    let last_mqtt = *state.last_heartbeat_mqtt.lock().unwrap();
    let last_zmq = *state.last_heartbeat_zmq.lock().unwrap();

    let mqtt_status = if now.duration_since(last_mqtt) < timeout { "online" } else { "offline" };
    let zmq_status = if now.duration_since(last_zmq) < timeout { "online" } else { "offline" };
    let uptime = now.duration_since(state.start_time).as_secs();

    Json(HealthStatus {
        mqtt_status: mqtt_status.to_string(),
        zeromq_status: zmq_status.to_string(),
        uptime_seconds: uptime,
        timestamp: chrono::Utc::now().to_rfc3339(),
    })
}

// Eliminar una alerta específica y sus archivos
async fn delete_alert_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Json<serde_json::Value> {
    // Recuperar paths primero
    let record = sqlx::query_as::<_, AlertRecord>("SELECT * FROM alerts WHERE id = ?")
        .bind(id)
        .fetch_optional(&state.db_pool)
        .await
        .unwrap_or(None);

    if let Some(rec) = record {
        // Borrar archivos físicos (ignoramos error si no existen)
        let _ = fs::remove_file(&rec.full_image_path);
        let _ = fs::remove_file(&rec.hotspot_image_path);

        // Borrar de DB
        let _ = sqlx::query("DELETE FROM alerts WHERE id = ?")
            .bind(id)
            .execute(&state.db_pool)
            .await;
            
        return Json(serde_json::json!({ "status": "deleted", "id": id }));
    }

    Json(serde_json::json!({ "status": "not_found" }))
}

// Eliminar TODAS las alertas y limpiar carpetas
async fn delete_all_alerts_handler(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    // Borrar registros
    let _ = sqlx::query("DELETE FROM alerts").execute(&state.db_pool).await;
    
    // Limpiar carpetas: Borrar directorio entero y re-crear
    let _ = fs::remove_dir_all("data/alerts/images/complete");
    let _ = fs::remove_dir_all("data/alerts/images/puntos_calientes");
    
    let _ = fs::create_dir_all("data/alerts/images/complete");
    let _ = fs::create_dir_all("data/alerts/images/puntos_calientes");

    Json(serde_json::json!({ "status": "all_deleted" }))
}