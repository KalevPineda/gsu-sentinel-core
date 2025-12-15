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

// --- CONFIGURACIÓN & ESTRUCTURAS ---

// Estado de la Máquina
#[derive(Clone, Debug, PartialEq)]
enum SystemMode {
    IdleAtEndpoint, // Esperando en un extremo
    Panning,        // Barrido normal
    Tracking,       // Centrando objetivo
    CollectingData, // Grabando dataset
    RemoteDisabled, // Apagado desde la Web
}

// Dirección del servo
#[derive(Clone, Debug, PartialEq)]
enum ScanDirection {
    RightToLeft,
    LeftToRight,
}

// Configuración que puede cambiar desde la Web
#[derive(Deserialize, Serialize, Clone, Debug)]
struct DynamicConfig {
    max_temp_trigger: f32,
    scan_wait_time_sec: u64,
    system_enabled: bool,
    pan_step_degrees: f32,
}

// Configuración fija (URLs, Hardware)
#[derive(Deserialize, Clone)]
struct StaticConfig {
    cloud_base_url: String,
    turbine_token: String,
    network: NetworkConfig,
}

#[derive(Deserialize, Clone)]
struct NetworkConfig {
    zmq_endpoint: String,
    mqtt_broker: String,
    mqtt_port: u16,
}

// Payload que enviamos cada segundo a la Web
#[derive(Serialize)]
struct HeartbeatPayload {
    turbine_token: String,
    mode: String,
    current_angle: f32,
    current_max_temp: f32,
    is_online: bool,
}

// Configuración completa combinada (archivo config.toml)
#[derive(Deserialize)]
struct FullConfigFile {
    static_conf: StaticConfig,
    initial_dynamic: DynamicConfig,
}

// Estado Global Compartido
struct AppState {
    mode: Arc<Mutex<SystemMode>>,
    angle: Arc<Mutex<f32>>,
    scan_direction: Arc<Mutex<ScanDirection>>,
    current_max_temp: Arc<Mutex<f32>>, // Para la UI
    
    // Configuración
    dynamic_conf: Arc<Mutex<DynamicConfig>>,
    static_conf: StaticConfig,
    
    // Buffer de datos para recolección
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
    
    let expected_len = rows * cols * 4; // float32 = 4 bytes
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
    // Ajustar PIXELS_PER_DEGREE según tu lente. Ej: 80 pixeles / 20 grados FOV = 4.0
    const PIXELS_PER_DEGREE: f32 = 4.5; 
    let error_pixels = (hotspot_x as f32) - center_x;
    // Retorna corrección en grados (invertir signo si el servo gira al revés)
    -(error_pixels / PIXELS_PER_DEGREE)
}

// --- MAIN ---

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    println!("🚀 Iniciando GSU Sentinel Core...");

    // 1. Cargar Configuración
    let config_content = std::fs::read_to_string("config.toml").unwrap_or_else(|_| {
        panic!("❌ Error: No se encontró 'config.toml'");
    });
    let loaded_config: FullConfigFile = toml::from_str(&config_content)?;
    
    // Crear directorios locales
    std::fs::create_dir_all("data/captures")?;
    std::fs::create_dir_all("data/logs")?;

    // 2. Inicializar Cliente HTTP
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    // 3. Inicializar MQTT
    let mut mqttoptions = MqttOptions::new(
        format!("gsu_core_{}", uuid::Uuid::new_v4()), 
        &loaded_config.static_conf.network.mqtt_broker, 
        loaded_config.static_conf.network.mqtt_port
    );
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    
    let (mqtt_client, mut mqtt_eventloop) = AsyncClient::new(mqttoptions, 10);
    
    // Tarea de fondo para MQTT
    tokio::spawn(async move {
        loop {
            if let Err(e) = mqtt_eventloop.poll().await {
                // Loguear error pero no saturar consola
                tracing::warn!("MQTT Error: {:?}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    });

    // 4. Construir Estado Global
    let state = Arc::new(AppState {
        mode: Arc::new(Mutex::new(SystemMode::IdleAtEndpoint)),
        angle: Arc::new(Mutex::new(90.0)), // Iniciar centrado
        scan_direction: Arc::new(Mutex::new(ScanDirection::RightToLeft)),
        current_max_temp: Arc::new(Mutex::new(0.0)),
        dynamic_conf: Arc::new(Mutex::new(loaded_config.initial_dynamic)),
        static_conf: loaded_config.static_conf.clone(),
        dataset_buffer: Arc::new(Mutex::new(Vec::new())),
        mqtt_client: mqtt_client.clone(),
        http_client: http_client.clone(),
    });

    // 5. TAREA: HEARTBEAT & SYNC (Comunicación con Web/Cloud)
    // Esta tarea corre independiente a la visión
    let state_hb = state.clone();
    tokio::spawn(async move {
        println!("📡 Iniciando servicio de Telemetría...");
        loop {
            // Recopilar datos (Scopes pequeños para soltar Mutex rápido)
            let (angle, temp, mode_desc) = {
                let a = *state_hb.angle.lock().await;
                let t = *state_hb.current_max_temp.lock().await;
                let m = state_hb.mode.lock().await;
                (a, t, format!("{:?}", *m))
            };

            let payload = HeartbeatPayload {
                turbine_token: state_hb.static_conf.turbine_token.clone(),
                mode: mode_desc,
                current_angle: angle,
                current_max_temp: temp,
                is_online: true,
            };

            let url = format!("{}/ingest/heartbeat", state_hb.static_conf.cloud_base_url);
            
            // Enviar POST y esperar nueva configuración
            match state_hb.http_client.post(&url).json(&payload).send().await {
                Ok(response) => {
                    if let Ok(new_cfg) = response.json::<DynamicConfig>().await {
                        let mut cfg_lock = state_hb.dynamic_conf.lock().await;
                        let mut mode_lock = state_hb.mode.lock().await;

                        // Actualizar valores
                        cfg_lock.max_temp_trigger = new_cfg.max_temp_trigger;
                        cfg_lock.scan_wait_time_sec = new_cfg.scan_wait_time_sec;
                        cfg_lock.pan_step_degrees = new_cfg.pan_step_degrees;

                        // Control Remoto ON/OFF
                        if !new_cfg.system_enabled {
                            if *mode_lock != SystemMode::RemoteDisabled {
                                println!("⛔ Sistema DESACTIVADO remotamente.");
                                *mode_lock = SystemMode::RemoteDisabled;
                            }
                        } else if *mode_lock == SystemMode::RemoteDisabled && new_cfg.system_enabled {
                            println!("✅ Sistema REACTIVADO remotamente.");
                            *mode_lock = SystemMode::IdleAtEndpoint;
                        }
                    }
                },
                Err(_) => {
                    // Fallo silencioso (Cloud offline), seguimos funcionando con config anterior
                }
            }

            tokio::time::sleep(Duration::from_millis(1000)).await; // 1Hz
        }
    });

    // 6. BUCLE PRINCIPAL DE VISIÓN (ZeroMQ)
    let zmq_url = loaded_config.static_conf.network.zmq_endpoint;
    let mut socket = zeromq::SubSocket::new();
    println!("👁️ Conectando a cámara en {}...", zmq_url);
    socket.connect(&zmq_url).await.expect("Fallo al conectar ZMQ");
    socket.subscribe("").await.expect("Fallo al suscribir ZMQ");

    // Variables locales para temporización
    let mut idle_timer = Instant::now();
    let mut collection_timer = Instant::now();
    let mut last_servo_update = Instant::now();
    
    // Límites físicos
    const LIMIT_LEFT: f32 = 125.0;
    const LIMIT_RIGHT: f32 = 55.0;

    println!("🟢 GSU Sentinel Core Operativo.");

    loop {
        // Recibir Frame (Await no bloquea Heartbeat)
        if let Ok(msg) = socket.recv().await {
            if let Some((timestamp, matrix)) = parse_zmq_msg(msg) {
                
                // --- A. Análisis Rápido ---
                let max_temp = *matrix.iter().reduce(|a, b| if a > b { a } else { b }).unwrap_or(&0.0);
                
                // Actualizar Temp Global (para Heartbeat)
                {
                    let mut t = state.current_max_temp.lock().await;
                    *t = max_temp;
                }

                // Obtener estado actual
                let mut mode = state.mode.lock().await;
                let mut angle = state.angle.lock().await;
                let mut direction = state.scan_direction.lock().await;
                let config = state.dynamic_conf.lock().await;

                // Si estamos apagados, saltamos el frame
                if *mode == SystemMode::RemoteDisabled {
                    continue;
                }

                // --- B. Lógica de Interrupción (Detección de Fuego) ---
                // Si vemos fuego y NO estamos recolectando ya, interrumpimos
                let is_collecting = matches!(*mode, SystemMode::CollectingData);
                if max_temp > config.max_temp_trigger && !is_collecting {
                     if !matches!(*mode, SystemMode::Tracking) {
                         println!("🔥 ALERTA DETECTADA ({}°C > {}°C). Iniciando Rastreo.", max_temp, config.max_temp_trigger);
                         *mode = SystemMode::Tracking;
                     }
                }

                // --- C. Máquina de Estados ---
                match *mode {
                    SystemMode::IdleAtEndpoint => {
                        // Esperar N segundos en el extremo
                        if idle_timer.elapsed().as_secs() >= config.scan_wait_time_sec {
                            println!("🔄 Iniciando Barrido...");
                            *mode = SystemMode::Panning;
                            // Invertir dirección
                            if *angle >= (LIMIT_LEFT - 5.0) {
                                *direction = ScanDirection::LeftToRight;
                            } else {
                                *direction = ScanDirection::RightToLeft;
                            }
                        }
                    },
                    SystemMode::Panning => {
                        // Mover servo paso a paso
                        let step = config.pan_step_degrees;
                        match *direction {
                            ScanDirection::RightToLeft => {
                                if *angle < LIMIT_LEFT { *angle += step; } 
                                else {
                                    *angle = LIMIT_LEFT;
                                    *mode = SystemMode::IdleAtEndpoint;
                                    idle_timer = Instant::now();
                                    println!("🛑 Extremo Izquierdo Alcanzado.");
                                }
                            },
                            ScanDirection::LeftToRight => {
                                if *angle > LIMIT_RIGHT { *angle -= step; } 
                                else {
                                    *angle = LIMIT_RIGHT;
                                    *mode = SystemMode::IdleAtEndpoint;
                                    idle_timer = Instant::now();
                                    println!("🛑 Extremo Derecho Alcanzado.");
                                }
                            }
                        }
                        // Limitar envío MQTT a ~20Hz para no saturar ESP32
                        if last_servo_update.elapsed().as_millis() > 50 {
                            publish_angle(&state.mqtt_client, *angle).await;
                            last_servo_update = Instant::now();
                        }
                    },
                    SystemMode::Tracking => {
                        // PID / Visual Servoing
                        let (max_idx, _) = matrix.iter().enumerate()
                            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap()).unwrap();
                        
                        let cols = matrix.ncols();
                        let hotspot_x = max_idx % cols;
                        
                        let correction = calculate_correction(hotspot_x, cols);
                        
                        // Si está centrado (error < 2 grados)
                        if correction.abs() < 2.0 {
                            println!("🎯 Objetivo centrado. Grabando evidencia...");
                            *mode = SystemMode::CollectingData;
                            collection_timer = Instant::now();
                            state.dataset_buffer.lock().await.clear();
                        } else {
                            // Corregir posición
                            *angle += correction * 0.4; // Factor P (Proporcional)
                            *angle = angle.clamp(0.0, 180.0);
                            publish_angle(&state.mqtt_client, *angle).await;
                        }

                        // Si se enfría, cancelar
                        if max_temp < (config.max_temp_trigger - 5.0) {
                            println!("❄️ Falsa alarma. Volviendo a Idle.");
                            *mode = SystemMode::IdleAtEndpoint;
                            idle_timer = Instant::now();
                        }
                    },
                    SystemMode::CollectingData => {
                        // Guardar frame en memoria
                        state.dataset_buffer.lock().await.push((timestamp, matrix.clone()));
                        
                        // Duración fija de grabación (ej. 3 segundos)
                        if collection_timer.elapsed().as_secs() >= 3 {
                            println!("✅ Captura finalizada. Procesando upload...");
                            
                            // Clonar datos para subir en background
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
                    SystemMode::RemoteDisabled => {
                        // Loop vacio
                    }
                }
            }
        }
    }
}

// --- FUNCIONES AUXILIARES ---

async fn publish_angle(client: &AsyncClient, angle: f32) {
    let payload = format!("{:.1}", angle);
    // QoS 0 es suficiente para streaming de posición
    let _ = client.publish("gsu/control/setpoint", QoS::AtMostOnce, false, payload).await;
}

async fn process_and_upload(state: Arc<AppState>, data: Vec<(u64, Array2<f32>)>, angle: f32) {
    if data.is_empty() { return; }
    
    // 1. Guardar localmente
    let timestamp = data[0].0;
    let filename = format!("data/captures/evidence_{}.npz", timestamp);
    
    // Buscar frame más caliente para guardar (simplificación)
    let best_frame = data.iter()
        .max_by(|a, b| {
            let max_a = a.1.fold(0./0., |m, v| v.max(m));
            let max_b = b.1.fold(0./0., |m, v| v.max(m));
            max_a.partial_cmp(&max_b).unwrap()
        })
        .unwrap();

    // Guardar .npz
    if let Ok(file) = std::fs::File::create(&filename) {
        if best_frame.1.write_npy(file).is_ok() {
             println!("💾 Evidencia guardada en disco: {}", filename);
        }
    }

    // 2. Subir al Cloud
    // Leer archivo como bytes
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
        
        match state.http_client.post(&url).multipart(form).send().await {
            Ok(_) => println!("☁️ Subida exitosa al Cloud."),
            Err(e) => eprintln!("⚠️ Error subiendo archivo: {}", e),
        }
    }
}