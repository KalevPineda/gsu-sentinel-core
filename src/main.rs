use axum::{
    extract::State,
    routing::get,
    Json, Router,
};
use byteorder::{LittleEndian, ReadBytesExt};
use ndarray::Array2;
use ndarray_npy::WriteNpyExt; 
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde::Deserialize;
use std::{io::Cursor, sync::Arc, time::{Duration, Instant}};
use tokio::sync::Mutex; 
use zeromq::{Socket, SocketRecv, ZmqMessage};

// --- CONFIGURACIÓN & ESTADOS ---

#[derive(Clone, Debug, PartialEq)]
enum SystemMode {
    IdleAtEndpoint, // Esperando 5s en GSU 1 o 2
    Panning,        // Moviéndose suavemente y buscando
    Tracking,       // Objetivo detectado, centrando
    CollectingData, // Grabando datos científicos
}

// Dirección del barrido
#[derive(Clone, Debug, PartialEq)]
enum ScanDirection {
    RightToLeft, // Yendo hacia 125
    LeftToRight, // Yendo hacia 55
}

struct AppState {
    current_mode: Arc<Mutex<SystemMode>>,
    current_angle: Arc<Mutex<f32>>,
    scan_direction: Arc<Mutex<ScanDirection>>, // Nueva variable de estado
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
    scan_wait_time_sec: u64,
    pan_step_degrees: f32,
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
    // Ajustar según FOV real de la cámara. Aprox 4.5 pixeles por grado.
    let pixels_per_degree = 4.5; 
    let error_pixels = (hotspot_x as f32) - center_x;
    // Invertimos signo según montaje del servo (ajustar si va al revés)
    -(error_pixels / pixels_per_degree)
}

fn parse_zmq_msg(msg: ZmqMessage) -> Option<(u64, Array2<f32>)> {
    if msg.len() < 2 { return None; }
    
    let header_bytes = msg.get(0)?;
    if header_bytes.len() < 20 { return None; }
    
    let mut rdr = Cursor::new(header_bytes);
    let timestamp = rdr.read_u64::<LittleEndian>().ok()?;
    let rows = rdr.read_i32::<LittleEndian>().ok()? as usize;
    let cols = rdr.read_i32::<LittleEndian>().ok()? as usize;

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

    // 1. Configuración
    let config_content = std::fs::read_to_string("config.toml").expect("Falta config.toml");
    let config: Config = toml::from_str(&config_content)?;
    
    std::fs::create_dir_all("data/captures")?;

    // 2. Clientes
    let cloud_client = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;

    let mut mqttoptions = MqttOptions::new("gsu_core_v3", &config.network.mqtt_broker, 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    let (mqtt_client, mut mqtt_eventloop) = AsyncClient::new(mqttoptions, 10);
    
    // 3. Estado Inicial
    let state = Arc::new(AppState {
        current_mode: Arc::new(Mutex::new(SystemMode::IdleAtEndpoint)),
        current_angle: Arc::new(Mutex::new(90.0)), // Iniciar al centro
        scan_direction: Arc::new(Mutex::new(ScanDirection::RightToLeft)),
        mqtt_client: mqtt_client.clone(),
        config: config.clone(),
        cloud_client,
        dataset_buffer: Arc::new(Mutex::new(Vec::new())),
    });

    // 4. MQTT Loop
    tokio::spawn(async move {
        loop { 
            if let Err(e) = mqtt_eventloop.poll().await {
                eprintln!("Error MQTT: {:?}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    });

    // 5. CEREBRO DE VISIÓN ARTIFICIAL
    let state_logic = state.clone();
    let zmq_endpoint = config.network.zmq_endpoint.clone();
    
    tokio::spawn(async move {
        let mut socket = zeromq::SubSocket::new();
        socket.connect(&zmq_endpoint).await.expect("Fallo ZMQ Connect");
        socket.subscribe("").await.expect("Fallo ZMQ Subscribe");

        // Timers locales
        let mut idle_start_time = Instant::now();
        let mut collection_start_time = Instant::now();
        
        // Límites del paneo (Grados del Servo)
        const ANGLE_LEFT_GSU1: f32 = 125.0;
        const ANGLE_RIGHT_GSU2: f32 = 55.0;

        println!("👁️  GSU Sentinel: Modo Panorámico Activo");

        // Bucle frame a frame (aprox 25 FPS dictado por la cámara)
        loop {
            match socket.recv().await {
                Ok(msg) => {
                    if let Some((timestamp, matrix)) = parse_zmq_msg(msg) {
                        
                        // 1. Análisis Inmediato (Cada frame)
                        let max_temp = *matrix.iter().reduce(|a, b| if a > b { a } else { b }).unwrap_or(&0.0);
                        
                        // Acceso seguro al estado
                        let mut mode = state_logic.current_mode.lock().await;
                        let mut angle = state_logic.current_angle.lock().await;
                        let mut direction = state_logic.scan_direction.lock().await;

                        // --- INTERRUPCIÓN DE ALERTA GLOBAL ---
                        // Si detectamos calor Y no estamos recolectando ya, interrumpimos todo para Trackear
                        let is_collecting = matches!(*mode, SystemMode::CollectingData);
                        if max_temp > state_logic.config.limits.max_temp_trigger && !is_collecting {
                            if !matches!(*mode, SystemMode::Tracking) {
                                println!("🔥 ALERTA ({}°C). Interrumpiendo paneo para centrar objetivo.", max_temp);
                                *mode = SystemMode::Tracking;
                            }
                        }

                        // --- MÁQUINA DE ESTADOS ---
                        match *mode {
                            SystemMode::IdleAtEndpoint => {
                                // Esperamos N segundos antes de volver a barrer
                                if idle_start_time.elapsed().as_secs() >= state_logic.config.limits.scan_wait_time_sec {
                                    println!("🔄 Tiempo de espera finalizado. Iniciando barrido...");
                                    *mode = SystemMode::Panning;
                                    
                                    // Invertir dirección para el siguiente barrido
                                    if *angle >= (ANGLE_LEFT_GSU1 - 5.0) {
                                        *direction = ScanDirection::LeftToRight;
                                    } else {
                                        *direction = ScanDirection::RightToLeft;
                                    }
                                }
                            },
                            SystemMode::Panning => {
                                // Mover el servo suavemente frame a frame
                                let step = state_logic.config.limits.pan_step_degrees;
                                
                                match *direction {
                                    ScanDirection::RightToLeft => {
                                        if *angle < ANGLE_LEFT_GSU1 {
                                            *angle += step;
                                        } else {
                                            // Llegamos al tope Izquierdo (GSU 1)
                                            *angle = ANGLE_LEFT_GSU1;
                                            *mode = SystemMode::IdleAtEndpoint;
                                            idle_start_time = Instant::now();
                                            println!("🛑 Llegada a GSU 1. Esperando...");
                                        }
                                    },
                                    ScanDirection::LeftToRight => {
                                        if *angle > ANGLE_RIGHT_GSU2 {
                                            *angle -= step;
                                        } else {
                                            // Llegamos al tope Derecho (GSU 2)
                                            *angle = ANGLE_RIGHT_GSU2;
                                            *mode = SystemMode::IdleAtEndpoint;
                                            idle_start_time = Instant::now();
                                            println!("🛑 Llegada a GSU 2. Esperando...");
                                        }
                                    }
                                }
                                
                                // Enviar comando al ESP32 (Suave)
                                publish_angle(&state_logic.mqtt_client, *angle).await;
                            },
                            SystemMode::Tracking => {
                                // Lógica de centrado (Visual Servoing)
                                let (max_idx, _) = matrix.iter().enumerate()
                                    .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap()).unwrap();
                                let cols = matrix.ncols();
                                let hotspot_x = max_idx % cols;
                                
                                let correction = calculate_correction(hotspot_x);
                                
                                if correction.abs() < 2.0 {
                                    // Centrado exitoso -> Grabar
                                    println!("🎯 Objetivo centrado. Iniciando recolección científica.");
                                    *mode = SystemMode::CollectingData;
                                    collection_start_time = Instant::now();
                                    state_logic.dataset_buffer.lock().await.clear();
                                } else {
                                    // Aplicar corrección PID visual
                                    *angle += correction * 0.3; // Ganancia suave
                                    *angle = angle.clamp(0.0, 180.0);
                                    publish_angle(&state_logic.mqtt_client, *angle).await;
                                }

                                // Si el punto se enfría o fue un falso positivo, volvemos a Idle
                                if max_temp < (state_logic.config.limits.max_temp_trigger - 2.0) {
                                    println!("❄️ Falsa alarma o enfriamiento. Volviendo a rutina.");
                                    *mode = SystemMode::IdleAtEndpoint;
                                    idle_start_time = Instant::now();
                                }
                            },
                            SystemMode::CollectingData => {
                                // Buffer circular / Acumulación
                                state_logic.dataset_buffer.lock().await.push((timestamp, matrix.clone()));
                                
                                // Verificar tiempo de recolección
                                if collection_start_time.elapsed().as_secs() >= state_logic.config.limits.collection_duration_sec {
                                    println!("✅ Recolección finalizada.");
                                    
                                    // Clonar y Subir (Async)
                                    let buffer = state_logic.dataset_buffer.lock().await.clone();
                                    let client = state_logic.cloud_client.clone();
                                    let url = state_logic.config.cloud_url.clone();
                                    let token = state_logic.config.turbine_token.clone();
                                    let current_ang = *angle;
                                    
                                    tokio::spawn(async move {
                                        process_and_upload(client, url, token, buffer, current_ang).await;
                                    });

                                    // Volver a esperar antes de seguir
                                    *mode = SystemMode::IdleAtEndpoint;
                                    idle_start_time = Instant::now();
                                }
                            }
                        }
                    } 
                },
                Err(e) => eprintln!("Error ZMQ: {:?}", e),
            }
        }
    });

    // 6. Servidor Web
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

// --- AUXILIARES ---

async fn publish_angle(client: &AsyncClient, angle: f32) {
    // Formatear a 1 decimal para no saturar el log del ESP32 con micro-cambios
    let payload = format!("{:.1}", angle);
    // QoS 0 (AtMostOnce) es preferible para streaming de posición suave (menor latencia)
    let _ = client.publish("gsu/control/setpoint", QoS::AtMostOnce, false, payload).await;
}

async fn process_and_upload(client: reqwest::Client, base_url: String, token: String, data: Vec<(u64, Array2<f32>)>, angle: f32) {
    if data.is_empty() { return; }
    let timestamp = data[0].0;
    let filename = format!("data/captures/dataset_{}.npz", timestamp);
    
    // Guardar frame de temperatura máxima
    let hottest_frame = data.iter()
        .max_by(|a, b| {
            let max_a = a.1.iter().fold(0.0/0.0, |m, v| v.max(m));
            let max_b = b.1.iter().fold(0.0/0.0, |m, v| v.max(m));
            max_a.partial_cmp(&max_b).unwrap()
        })
        .unwrap();

    match std::fs::File::create(&filename) {
        Ok(file) => {
            let _ = hottest_frame.1.write_npy(file);
            println!("💾 Datos guardados en disco.");
        },
        Err(e) => eprintln!("Error guardando archivo: {}", e),
    }

    match tokio::fs::read(&filename).await {
        Ok(bytes) => {
            let part = reqwest::multipart::Part::bytes(bytes).file_name(filename.clone()).mime_str("application/octet-stream").unwrap();
            let form = reqwest::multipart::Form::new()
                .text("turbine_token", token)
                .text("angle", angle.to_string())
                .part("dataset_file", part);

            let _ = client.post(format!("{}/ingest/upload", base_url)).multipart(form).send().await;
            println!("☁️ Datos sincronizados con la nube.");
        },
        Err(_) => {},
    }
}

async fn health_handler() -> Json<&'static str> { Json("online") }

async fn status_handler(State(state): State<Arc<AppState>>) -> Json<String> {
    let mode = state.current_mode.lock().await;
    let angle = state.current_angle.lock().await;
    Json(format!("Mode: {:?} | Angle: {:.1}", *mode, *angle))
}