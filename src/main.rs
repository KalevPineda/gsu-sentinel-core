use byteorder::{LittleEndian, ReadBytesExt};
use ndarray::Array2;
use ndarray_npy::WriteNpyExt;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use std::{
    io::Cursor,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use zeromq::{Socket, SocketRecv, ZmqMessage};

// --- ESTRUCTURAS DE ESTADO Y CONFIGURACIÓN ---

// Estado de la Máquina Local
#[derive(Clone, Debug, PartialEq)]
enum SystemMode {
    IdleAtEndpoint,
    Panning,
    Tracking,
    CollectingData,
    RemoteDisabled,
}

#[derive(Clone, Debug, PartialEq)]
enum ScanDirection {
    RightToLeft,
    LeftToRight,
}

// Configuración Dinámica (Debe coincidir con RemoteConfig del Cloud)
#[derive(Deserialize, Serialize, Clone, Debug)]
struct RemoteConfig {
    max_temp_trigger: f32,
    scan_wait_time_sec: u64,
    system_enabled: bool,
    pan_step_degrees: f32,
}

// Configuración Estática (Redes, Hardware)
#[derive(Deserialize, Clone)]
struct StaticConfig {
    cloud_base_url: String, // Aquí va la IP 192.168.0.4
    turbine_token: String,
    network: NetworkConfig,
}

#[derive(Deserialize, Clone)]
struct NetworkConfig {
    zmq_endpoint: String,
    mqtt_broker: String,
    mqtt_port: u16,
}

// Payload para el Heartbeat (Debe coincidir con LiveStatus del Cloud)
#[derive(Serialize)]
struct LiveStatusPayload {
    last_update: u64, // Importante: Cloud lo espera aunque lo sobrescriba
    turbine_token: String,
    mode: String,
    current_angle: f32,
    current_max_temp: f32,
    is_online: bool,
}

#[derive(Deserialize)]
struct FullConfigFile {
    static_conf: StaticConfig,
    initial_dynamic: RemoteConfig,
}

// Estado Global Compartido
struct AppState {
    mode: Arc<Mutex<SystemMode>>,
    angle: Arc<Mutex<f32>>,
    scan_direction: Arc<Mutex<ScanDirection>>,
    current_max_temp: Arc<Mutex<f32>>,
    
    // Configuración sincronizada
    dynamic_conf: Arc<Mutex<RemoteConfig>>,
    static_conf: StaticConfig,
    
    // Buffer
    dataset_buffer: Arc<Mutex<Vec<(u64, Array2<f32>)>>>,
    
    // Clientes
    mqtt_client: AsyncClient,
    http_client: reqwest::Client,
}

// --- UTILIDADES ---

fn parse_zmq_msg(msg: ZmqMessage) -> Option<(u64, Array2<f32>)> {
    if msg.len() < 2 { return None; }
    let header = msg.get(0)?;
    let payload = msg.get(1)?;
    
    if header.len() < 20 { return None; }
    
    let mut rdr = Cursor::new(header);
    let timestamp = rdr.read_u64::<LittleEndian>().ok()?;
    let rows = rdr.read_i32::<LittleEndian>().ok()? as usize;
    let cols = rdr.read_i32::<LittleEndian>().ok()? as usize;
    
    let expected_len = rows * cols * 4; 
    if payload.len() != expected_len { return None; }

    let mut floats = vec![0.0f32; rows * cols];
    let mut rdr_payload = Cursor::new(payload);
    if rdr_payload.read_f32_into::<LittleEndian>(&mut floats).is_err() {
        return None;
    }

    let matrix = Array2::from_shape_vec((rows, cols), floats).ok()?;
    Some((timestamp, matrix))
}

fn calculate_correction(hotspot_x: usize, width: usize) -> f32 {
    let center_x = width as f32 / 2.0;
    const PIXELS_PER_DEGREE: f32 = 4.5; 
    let error_pixels = (hotspot_x as f32) - center_x;
    -(error_pixels / PIXELS_PER_DEGREE)
}

// --- MAIN ---

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    println!("🚀 Iniciando GSU Sentinel Core...");

    // 1. Cargar Configuración
    let config_content = std::fs::read_to_string("config.toml").expect("❌ Error: Falta config.toml");
    let loaded_config: FullConfigFile = toml::from_str(&config_content)?;
    
    std::fs::create_dir_all("data/captures")?;

    // 2. Cliente HTTP
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;

    // 3. Cliente MQTT
    let mut mqttoptions = MqttOptions::new(
        format!("gsu_core_{}", uuid::Uuid::new_v4()), 
        &loaded_config.static_conf.network.mqtt_broker, 
        loaded_config.static_conf.network.mqtt_port
    );
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    let (mqtt_client, mut mqtt_eventloop) = AsyncClient::new(mqttoptions, 10);
    
    tokio::spawn(async move {
        loop {
            if let Err(_) = mqtt_eventloop.poll().await {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    });

    // 4. Estado Global
    let state = Arc::new(AppState {
        mode: Arc::new(Mutex::new(SystemMode::IdleAtEndpoint)),
        angle: Arc::new(Mutex::new(90.0)),
        scan_direction: Arc::new(Mutex::new(ScanDirection::RightToLeft)),
        current_max_temp: Arc::new(Mutex::new(0.0)),
        dynamic_conf: Arc::new(Mutex::new(loaded_config.initial_dynamic)),
        static_conf: loaded_config.static_conf.clone(),
        dataset_buffer: Arc::new(Mutex::new(Vec::new())),
        mqtt_client: mqtt_client.clone(),
        http_client: http_client.clone(),
    });

    // --- TAREA HEARTBEAT (Aquí solucionamos el problema del LIVE) ---
    let state_hb = state.clone();
    tokio::spawn(async move {
        println!("📡 Iniciando Telemetría hacia: {}", state_hb.static_conf.cloud_base_url);
        loop {
            let (angle, temp, mode_desc) = {
                let a = *state_hb.angle.lock().await;
                let t = *state_hb.current_max_temp.lock().await;
                let m = state_hb.mode.lock().await;
                (a, t, format!("{:?}", *m))
            };

            let payload = LiveStatusPayload {
                last_update: 0, // El cloud lo ignorará, pero debe estar presente en el JSON
                turbine_token: state_hb.static_conf.turbine_token.clone(),
                mode: mode_desc,
                current_angle: angle,
                current_max_temp: temp,
                is_online: true,
            };

            let url = format!("{}/ingest/heartbeat", state_hb.static_conf.cloud_base_url);
            
            // Enviamos POST
            match state_hb.http_client.post(&url).json(&payload).send().await {
                Ok(response) => {
                    // Si responde OK, actualizamos configuración local
                    if let Ok(new_cfg) = response.json::<RemoteConfig>().await {
                        let mut cfg_lock = state_hb.dynamic_conf.lock().await;
                        let mut mode_lock = state_hb.mode.lock().await;

                        cfg_lock.max_temp_trigger = new_cfg.max_temp_trigger;
                        cfg_lock.scan_wait_time_sec = new_cfg.scan_wait_time_sec;
                        cfg_lock.pan_step_degrees = new_cfg.pan_step_degrees;

                        // Lógica ON/OFF remoto
                        if !new_cfg.system_enabled {
                            if *mode_lock != SystemMode::RemoteDisabled {
                                println!("⛔ Sistema PAUSADO remotamente.");
                                *mode_lock = SystemMode::RemoteDisabled;
                            }
                        } else if *mode_lock == SystemMode::RemoteDisabled && new_cfg.system_enabled {
                            println!("✅ Sistema REANUDADO remotamente.");
                            *mode_lock = SystemMode::IdleAtEndpoint;
                        }
                    }
                },
                Err(e) => {
                    // Solo imprimimos error si es persistente para no saturar consola
                    // println!("⚠️ Error Heartbeat: {}", e); 
                }
            }

            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    });

    // 5. Bucle de Visión (ZMQ)
    let zmq_url = loaded_config.static_conf.network.zmq_endpoint;
    let mut socket = zeromq::SubSocket::new();
    socket.connect(&zmq_url).await.expect("Fallo ZMQ");
    socket.subscribe("").await.expect("Fallo Subscribe");

    let mut idle_timer = Instant::now();
    let mut collection_timer = Instant::now();
    let mut last_servo_update = Instant::now();
    
    // Límites Servo
    const LIMIT_LEFT: f32 = 125.0;
    const LIMIT_RIGHT: f32 = 55.0;

    println!("🟢 GSU Core Operativo. Esperando frames...");

    loop {
        if let Ok(msg) = socket.recv().await {
            if let Some((timestamp, matrix)) = parse_zmq_msg(msg) {
                
                // Análisis
                let max_temp = *matrix.iter().reduce(|a, b| if a > b { a } else { b }).unwrap_or(&0.0);
                
                // Actualizar valor para la Web
                {
                    let mut t = state.current_max_temp.lock().await;
                    *t = max_temp;
                }

                let mut mode = state.mode.lock().await;
                let mut angle = state.angle.lock().await;
                let mut direction = state.scan_direction.lock().await;
                let config = state.dynamic_conf.lock().await;

                if *mode == SystemMode::RemoteDisabled { continue; }

                // Detector de Alerta
                let is_collecting = matches!(*mode, SystemMode::CollectingData);
                if max_temp > config.max_temp_trigger && !is_collecting {
                     if !matches!(*mode, SystemMode::Tracking) {
                         println!("🔥 ALERTA ({}°C). Rastreando...", max_temp);
                         *mode = SystemMode::Tracking;
                     }
                }

                // Máquina de Estados
                match *mode {
                    SystemMode::IdleAtEndpoint => {
                        if idle_timer.elapsed().as_secs() >= config.scan_wait_time_sec {
                            *mode = SystemMode::Panning;
                            if *angle >= (LIMIT_LEFT - 5.0) {
                                *direction = ScanDirection::LeftToRight;
                            } else {
                                *direction = ScanDirection::RightToLeft;
                            }
                        }
                    },
                    SystemMode::Panning => {
                        let step = config.pan_step_degrees;
                        match *direction {
                            ScanDirection::RightToLeft => {
                                if *angle < LIMIT_LEFT { *angle += step; } 
                                else {
                                    *angle = LIMIT_LEFT;
                                    *mode = SystemMode::IdleAtEndpoint;
                                    idle_timer = Instant::now();
                                }
                            },
                            ScanDirection::LeftToRight => {
                                if *angle > LIMIT_RIGHT { *angle -= step; } 
                                else {
                                    *angle = LIMIT_RIGHT;
                                    *mode = SystemMode::IdleAtEndpoint;
                                    idle_timer = Instant::now();
                                }
                            }
                        }
                        
                        if last_servo_update.elapsed().as_millis() > 50 {
                            publish_angle(&state.mqtt_client, *angle).await;
                            last_servo_update = Instant::now();
                        }
                    },
                    SystemMode::Tracking => {
                        let (max_idx, _) = matrix.iter().enumerate()
                            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap()).unwrap();
                        let cols = matrix.ncols();
                        let hotspot_x = max_idx % cols;
                        let correction = calculate_correction(hotspot_x, cols);
                        
                        if correction.abs() < 2.0 {
                            *mode = SystemMode::CollectingData;
                            collection_timer = Instant::now();
                            state.dataset_buffer.lock().await.clear();
                        } else {
                            *angle += correction * 0.4;
                            *angle = angle.clamp(0.0, 180.0);
                            publish_angle(&state.mqtt_client, *angle).await;
                        }

                        if max_temp < (config.max_temp_trigger - 5.0) {
                            *mode = SystemMode::IdleAtEndpoint;
                            idle_timer = Instant::now();
                        }
                    },
                    SystemMode::CollectingData => {
                        state.dataset_buffer.lock().await.push((timestamp, matrix.clone()));
                        
                        if collection_timer.elapsed().as_secs() >= 3 {
                            println!("✅ Recolección finalizada. Subiendo...");
                            let buffer_copy = state.dataset_buffer.lock().await.clone();
                            let state_upload = state.clone();
                            let current_angle = *angle;
                            
                            tokio::spawn(async move {
                                process_and_upload(state_upload, buffer_copy, current_angle).await;
                            });

                            *mode = SystemMode::IdleAtEndpoint;
                            idle_timer = Instant::now();
                        }
                    },
                    _ => {}
                }
            }
        }
    }
}

async fn publish_angle(client: &AsyncClient, angle: f32) {
    let payload = format!("{:.1}", angle);
    let _ = client.publish("gsu/control/setpoint", QoS::AtMostOnce, false, payload).await;
}

async fn process_and_upload(state: Arc<AppState>, data: Vec<(u64, Array2<f32>)>, angle: f32) {
    if data.is_empty() { return; }
    
    let timestamp = data[0].0;
    let filename = format!("data/captures/evidence_{}.npz", timestamp);
    
    // Guardar temporalmente
    let best_frame = data[data.len() / 2].clone(); // Tomar frame central aprox
    if let Ok(file) = std::fs::File::create(&filename) {
        let _ = best_frame.1.write_npy(file);
    }

    // Subir
    if let Ok(file_bytes) = tokio::fs::read(&filename).await {
        let part = reqwest::multipart::Part::bytes(file_bytes)
            .file_name(filename.clone())
            .mime_str("application/octet-stream")
            .unwrap();

        let form = reqwest::multipart::Form::new()
            .text("turbine_token", state.static_conf.turbine_token.clone())
            .text("angle", angle.to_string())
            .part("dataset_file", part);

        let url = format!("{}/ingest/upload", state.static_conf.cloud_base_url);
        let _ = state.http_client.post(&url).multipart(form).send().await;
    }
}