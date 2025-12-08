

use axum::{
    extract::State,
    routing::get,
    Json, Router,
};
use byteorder::{LittleEndian, ReadBytesExt};
use ndarray::Array2;
use ndarray_npy::WriteNpyExt; 
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde::Deserialize; // Eliminado Serialize no usado
use std::{io::Cursor, sync::Arc, time::{Duration, Instant}};
use tokio::sync::Mutex; // Eliminado broadcast no usado
use zeromq::{Socket, SocketRecv, ZmqMessage}; // ZmqMessage sí se usa en la firma de la función

// --- CONFIGURACIÓN & ESTADOS ---

#[derive(Clone, Debug, PartialEq)]
enum SystemMode {
    Scanning,       
    Tracking,       
    CollectingData, 
}

struct AppState {
    current_mode: Arc<Mutex<SystemMode>>,
    current_angle: Arc<Mutex<f32>>,
    mqtt_client: AsyncClient,
    config: Config,
    cloud_client: reqwest::Client,
    dataset_buffer: Arc<Mutex<Vec<(u64, Array2<f32>)>>>, 
}

#[derive(Deserialize, Clone)]
struct Config {
    cloud_url: String, 
    turbine_token: String,
    limits: Limits,
    network: NetworkConfig,
}

#[derive(Deserialize, Clone)]
struct Limits {
    max_temp_trigger: f32, 
    collection_duration_sec: u64,
}

#[derive(Deserialize, Clone)]
struct NetworkConfig {
    zmq_endpoint: String,
    mqtt_broker: String,
    http_port: u16,
}

// --- UTILIDADES ---

fn calculate_correction(hotspot_x: usize) -> f32 {
    let center_x = 128.0;
    let pixels_per_degree = 4.5; 
    let error_pixels = (hotspot_x as f32) - center_x;
    -(error_pixels / pixels_per_degree)
}

fn parse_zmq_msg(msg: ZmqMessage) -> Option<(u64, Array2<f32>)> {
    if msg.len() < 2 { return None; }
    
    // 1. Parsear Header
    let header_bytes = msg.get(0)?;
    if header_bytes.len() < 20 { return None; }
    
    let mut rdr = Cursor::new(header_bytes);
    let timestamp = rdr.read_u64::<LittleEndian>().ok()?;
    let rows = rdr.read_i32::<LittleEndian>().ok()? as usize;
    let cols = rdr.read_i32::<LittleEndian>().ok()? as usize;

    // 2. Parsear Payload
    let payload_bytes = msg.get(1)?;
    let expected_len = rows * cols * 4; 
    
    if payload_bytes.len() != expected_len { return None; }

    let mut floats = vec![0.0f32; rows * cols];
    let mut rdr_payload = Cursor::new(payload_bytes);
    if let Err(_) = rdr_payload.read_f32_into::<LittleEndian>(&mut floats) {
        return None;
    }

    let matrix = Array2::from_shape_vec((rows, cols), floats).ok()?;
    Some((timestamp, matrix))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // 1. Cargar Configuración
    let config_content = std::fs::read_to_string("config.toml").expect("Falta config.toml");
    let config: Config = toml::from_str(&config_content)?;
    
    std::fs::create_dir_all("data/captures")?;

    // 2. Cliente Cloud
    let cloud_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;

    // 3. MQTT
    let mut mqttoptions = MqttOptions::new("gsu_core_v3", &config.network.mqtt_broker, 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    let (mqtt_client, mut mqtt_eventloop) = AsyncClient::new(mqttoptions, 10);
    
    // 4. Estado Global
    let state = Arc::new(AppState {
        current_mode: Arc::new(Mutex::new(SystemMode::Scanning)),
        current_angle: Arc::new(Mutex::new(90.0)),
        mqtt_client: mqtt_client.clone(),
        config: config.clone(),
        cloud_client,
        dataset_buffer: Arc::new(Mutex::new(Vec::new())),
    });

    // 5. Loop MQTT
    tokio::spawn(async move {
        loop { 
            if let Err(e) = mqtt_eventloop.poll().await {
                eprintln!("Error MQTT: {:?}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    });

    // 6. Loop de Visión
    let state_logic = state.clone();
    let zmq_endpoint = config.network.zmq_endpoint.clone();
    
    tokio::spawn(async move {
        let mut socket = zeromq::SubSocket::new();
        socket.connect(&zmq_endpoint).await.expect("Fallo ZMQ Connect");
        socket.subscribe("").await.expect("Fallo ZMQ Subscribe");

        let mut last_scan_move = Instant::now();
        let mut collection_start = Instant::now();
        let mut scan_target = 0; 

        println!("👁️ Sistema de Visión Iniciado. Esperando frames...");

        loop {
            match socket.recv().await {
                Ok(msg) => {
                    if let Some((timestamp, matrix)) = parse_zmq_msg(msg) {
                        let max_temp = *matrix.iter().reduce(|a, b| if a > b { a } else { b }).unwrap_or(&0.0);
                        
                        let mut mode_guard = state_logic.current_mode.lock().await;
                        let mut angle_guard = state_logic.current_angle.lock().await;

                        match *mode_guard {
                            SystemMode::Scanning => {
                                if max_temp > state_logic.config.limits.max_temp_trigger {
                                    println!("🔥 DETECCIÓN: {}°C. Iniciando Tracking.", max_temp);
                                    *mode_guard = SystemMode::Tracking;
                                } else {
                                    if last_scan_move.elapsed().as_secs() > 5 {
                                        if scan_target == 0 {
                                            *angle_guard = 55.0; 
                                            scan_target = 1;
                                        } else {
                                            *angle_guard = 125.0; 
                                            scan_target = 0;
                                        }
                                        publish_angle(&state_logic.mqtt_client, *angle_guard).await;
                                        last_scan_move = Instant::now();
                                    }
                                }
                            },
                            SystemMode::Tracking => {
                                let (max_idx, _) = matrix.iter().enumerate()
                                    .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap()).unwrap();
                                
                                let cols = matrix.ncols();
                                let hotspot_x = max_idx % cols;
                                let correction = calculate_correction(hotspot_x);
                                
                                if correction.abs() < 2.0 {
                                    println!("🎯 Objetivo Centrado. Recolectando datos...");
                                    *mode_guard = SystemMode::CollectingData;
                                    collection_start = Instant::now();
                                    state_logic.dataset_buffer.lock().await.clear();
                                } else {
                                    *angle_guard += correction * 0.4;
                                    *angle_guard = angle_guard.clamp(0.0, 180.0);
                                    publish_angle(&state_logic.mqtt_client, *angle_guard).await;
                                }
                                
                                if max_temp < (state_logic.config.limits.max_temp_trigger - 2.0) {
                                     *mode_guard = SystemMode::Scanning;
                                }
                            },
                            SystemMode::CollectingData => {
                                state_logic.dataset_buffer.lock().await.push((timestamp, matrix.clone()));
                                
                                if collection_start.elapsed().as_secs() > state_logic.config.limits.collection_duration_sec {
                                    println!("✅ Dataset finalizado. Guardando y subiendo...");
                                    
                                    let buffer_copy = state_logic.dataset_buffer.lock().await.clone();
                                    let client = state_logic.cloud_client.clone();
                                    let url = state_logic.config.cloud_url.clone();
                                    let token = state_logic.config.turbine_token.clone();
                                    let angle_snap = *angle_guard;
                                    
                                    tokio::spawn(async move {
                                        process_and_upload(client, url, token, buffer_copy, angle_snap).await;
                                    });

                                    *mode_guard = SystemMode::Scanning; 
                                }
                            }
                        }
                    } 
                },
                Err(e) => eprintln!("Error ZMQ Recv: {:?}", e),
            }
        }
    });

    // 7. Servidor HTTP
    let app = Router::new()
        .route("/api/health", get(health_handler))
        .route("/api/status", get(status_handler))
        .with_state(state.clone());

    let addr = format!("0.0.0.0:{}", config.network.http_port);
    println!("🟢 GSU Core corriendo en {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

// --- HANDLERS & LOGICA AUXILIAR ---

async fn publish_angle(client: &AsyncClient, angle: f32) {
    let payload = format!("{:.2}", angle);
    let _ = client.publish("gsu/control/setpoint", QoS::AtLeastOnce, false, payload).await;
}

async fn process_and_upload(client: reqwest::Client, base_url: String, token: String, data: Vec<(u64, Array2<f32>)>, angle: f32) {
    if data.is_empty() { return; }
    
    let timestamp = data[0].0;
    let filename = format!("data/captures/dataset_{}.npz", timestamp);
    
    // Obtenemos el frame más caliente
    let hottest_frame = data.iter()
        .max_by(|a, b| {
            let max_a = a.1.iter().fold(0.0/0.0, |m, v| v.max(m));
            let max_b = b.1.iter().fold(0.0/0.0, |m, v| v.max(m));
            max_a.partial_cmp(&max_b).unwrap()
        })
        .unwrap();

    // --- CORRECCIÓN AQUÍ: Crear archivo físico primero ---
    match std::fs::File::create(&filename) {
        Ok(file) => {
            if let Err(e) = hottest_frame.1.write_npy(file) {
                eprintln!("Error escribiendo NPZ: {}", e);
                return;
            }
            println!("💾 Archivo científico guardado: {}", filename);
        },
        Err(e) => {
            eprintln!("No se pudo crear el archivo {}: {}", filename, e);
            return;
        }
    }

    // Subir a la nube
    match tokio::fs::read(&filename).await {
        Ok(file_bytes) => {
            let part = reqwest::multipart::Part::bytes(file_bytes)
                .file_name(filename.clone())
                .mime_str("application/octet-stream").unwrap();

            let form = reqwest::multipart::Form::new()
                .text("turbine_token", token)
                .text("angle", angle.to_string())
                .part("dataset_file", part);

            match client.post(format!("{}/ingest/upload", base_url))
                .multipart(form)
                .send()
                .await {
                    Ok(_) => println!("☁️ Upload exitoso."),
                    Err(e) => eprintln!("❌ Falló upload a la nube: {}", e),
                }
        },
        Err(_) => eprintln!("No se pudo leer el archivo para subir."),
    }
}

async fn health_handler() -> Json<&'static str> {
    Json("online")
}

async fn status_handler(State(state): State<Arc<AppState>>) -> Json<String> {
    let mode = state.current_mode.lock().await;
    let angle = state.current_angle.lock().await;
    Json(format!("Mode: {:?}, Angle: {:.2}", *mode, *angle))
}