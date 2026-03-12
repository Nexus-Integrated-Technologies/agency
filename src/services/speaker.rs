use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use anyhow::{Context, Result};
use candle_core::{Device, Tensor};
use candle_nn::Embedding;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokenizers::Tokenizer;
use tracing::{debug, error, info};

// Reuse the model logic from the library
use crate::models::t3_candle::T3Candle;

const DEFAULT_ARTIFACT_SUBDIR: &str = "artifacts/chatterbox";
const DEFAULT_DEVICE: &str = "cpu";
const MODEL_POOL_SIZE: usize = 2;
const SAMPLE_RATE_HZ: u32 = 24_000;
const NATURAL_GAP_SAMPLES: usize = 1_200;
const MIN_SPEECH_TOKENS: usize = 24;
const DECODER_PAD_TOKEN: i64 = 4_299;
const START_TOKEN: i64 = 6_561;
const STOP_TOKEN: i64 = 6_562;

fn resolve_artifact_dir() -> PathBuf {
    env::var("AGENCY_ARTIFACT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_ARTIFACT_SUBDIR))
}

fn resolve_t3_device() -> Device {
    let requested = env::var("AGENCY_DEVICE").unwrap_or_else(|_| DEFAULT_DEVICE.to_string());
    match requested.to_ascii_lowercase().as_str() {
        "metal" => Device::new_metal(0).unwrap_or(Device::Cpu),
        "cuda" => Device::new_cuda(0).unwrap_or(Device::Cpu),
        _ => Device::Cpu,
    }
}

struct ModelPool {
    receiver: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<T3Candle>>>,
    sender: mpsc::UnboundedSender<T3Candle>,
}

impl ModelPool {
    fn new(models: Vec<T3Candle>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        for model in models {
            let _ = tx.send(model);
        }
        Self {
            receiver: Arc::new(tokio::sync::Mutex::new(rx)),
            sender: tx,
        }
    }

    async fn checkout(&self) -> Result<T3Candle> {
        let mut rx = self.receiver.lock().await;
        rx.recv().await.context("Model pool exhausted")
    }

    fn checkin(&self, model: T3Candle) {
        let _ = self.sender.send(model);
    }
}

pub struct AudioEngine {
    decoder_model: Arc<candle_onnx::onnx::ModelProto>,
    tokenizer: Tokenizer,
    model_pool: Arc<ModelPool>,
    speech_emb: Embedding,
    start_token: i64,
    stop_token: i64,
    device: Device,
    decoder_device: Device,
    sink: Arc<rodio::Sink>,
}

impl AudioEngine {
    pub fn new(sink: Arc<rodio::Sink>) -> Result<Self> {
        let artifact_dir = resolve_artifact_dir();
        let device = resolve_t3_device();

        let decoder_device = Device::Cpu;
        info!("AudioEngine: Loading Engine (T3: {:?}, Decoder: {:?})", device, decoder_device);

        let config = crate::models::t3::Config::t3_turbo();
        let raw_weights = candle_core::safetensors::load(artifact_dir.join("speaker_weights_q8.safetensors"), &device)?;
        
        let mut t3_weights = HashMap::new();
        for (name, tensor) in raw_weights {
            let dt = tensor.dtype();
            if dt.is_float() {
                t3_weights.insert(name, tensor.to_dtype(candle_core::DType::F32)?);
            } else {
                t3_weights.insert(name, tensor);
            }
        }

        let mut models = Vec::new();
        for i in 0..MODEL_POOL_SIZE {
            debug!("AudioEngine: Initializing model instance {}...", i);
            models.push(T3Candle::load_from_map(&t3_weights, &config, &device)?);
        }
        let model_pool = Arc::new(ModelPool::new(models));

        let tokenizer = Tokenizer::from_file(artifact_dir.join("tokenizer.json"))
            .map_err(|e| anyhow::anyhow!("Tokenizer error: {}", e))?;

        let speech_emb = crate::models::t3_candle::load_embedding(
            &t3_weights, "speech_emb", 6563, 1024, &device
        )?;

        let decoder_model = candle_onnx::read_file(artifact_dir.join("conditional_decoder_q8_full.onnx"))?;

        Ok(Self {
            decoder_model: Arc::new(decoder_model),
            tokenizer,
            model_pool,
            speech_emb,
            start_token: START_TOKEN,
            stop_token: STOP_TOKEN,
            device,
            decoder_device,
            sink,
        })
    }

    pub async fn synthesize(&self, text: String) -> Result<()> {
        let (audio_tx, mut audio_rx) = mpsc::unbounded_channel::<(usize, Vec<f32>)>();
        
        let re = regex::Regex::new(r"(?s)[^.!?\n\r,;:]+[.!?\n\r,;:]*")?;
        let sentences: Vec<String> = re.find_iter(&text) 
            .map(|m| m.as_str().trim().to_string())
            .filter(|s| !s.is_empty() && s.len() > 1)
            .collect();

        if sentences.is_empty() { return Ok(()); }
        info!("AudioEngine: Synthesizing {} chunks...", sentences.len());

        for (idx, sentence) in sentences.into_iter().enumerate() {
            let audio_tx = audio_tx.clone();
            let pool = self.model_pool.clone();
            let decoder_model = self.decoder_model.clone();
            let speech_emb = self.speech_emb.clone();
            let device = self.device.clone();
            let decoder_device = self.decoder_device.clone();
            let start_token = self.start_token;
            let stop_token = self.stop_token;
            let tokenizer = self.tokenizer.clone();

            tokio::spawn(async move {
                let mut model = match pool.checkout().await {
                    Ok(model) => model,
                    Err(e) => {
                        error!("AudioEngine: Failed to checkout model for chunk {}: {}", idx, e);
                        let _ = audio_tx.send((idx, Vec::new()));
                        return;
                    }
                };
                let start_time = std::time::Instant::now();
                
                let result = tokio::task::spawn_blocking(move || {
                    let inference_result = (|| -> Result<Vec<f32>> {
                        let tokens = T3Candle::generate_tokens_internal_static(
                            &mut model,
                            &tokenizer,
                            &sentence,
                            &speech_emb,
                            &device,
                            start_token,
                            stop_token,
                        )?;

                        Self::decode_audio_native_static(&decoder_model, &tokens, &decoder_device)
                    })();
                    (inference_result, model)
                }).await;

                match result {
                    Ok((Ok(audio), model)) => {
                        let _ = audio_tx.send((idx, audio));
                        debug!("AudioEngine: Chunk {} done in {}ms", idx, start_time.elapsed().as_millis());
                        pool.checkin(model);
                    }
                    Ok((Err(e), model)) => {
                        error!("AudioEngine: Inference error on chunk {}: {}", idx, e);
                        let _ = audio_tx.send((idx, Vec::new()));
                        pool.checkin(model);
                    }
                    Err(e) => {
                        error!("AudioEngine: Task join error on chunk {}: {}", idx, e);
                        let _ = audio_tx.send((idx, Vec::new()));
                    }
                }
            });
        }
        drop(audio_tx);

        let sink = self.sink.clone();
        tokio::spawn(async move {
            let mut pending = HashMap::new();
            let mut next_to_play = 0;
            
            while let Some((idx, audio)) = audio_rx.recv().await {
                pending.insert(idx, audio);
                
                while let Some(audio) = pending.remove(&next_to_play) {
                    if !audio.is_empty() {
                        let source = rodio::buffer::SamplesBuffer::new(1, SAMPLE_RATE_HZ, audio);
                        sink.append(source);
                        let gap = rodio::buffer::SamplesBuffer::new(1, SAMPLE_RATE_HZ, vec![0.0f32; NATURAL_GAP_SAMPLES]);
                        sink.append(gap);
                    }
                    next_to_play += 1;
                }
            }
        });

        Ok(())
    }

    fn decode_audio_native_static(
        decoder_model: &candle_onnx::onnx::ModelProto, 
        tokens: &[i64], 
        device: &Device
    ) -> Result<Vec<f32>> {
        let mut speech_tokens: Vec<i64> = tokens.iter() 
            .cloned() 
            .filter(|&t| t < START_TOKEN)
            .collect();

        if speech_tokens.is_empty() { return Ok(Vec::new()); }
        while speech_tokens.len() < MIN_SPEECH_TOKENS {
            speech_tokens.push(DECODER_PAD_TOKEN);
        }
        for _ in 0..3 {
            speech_tokens.push(DECODER_PAD_TOKEN);
        }
        
        let total_len = speech_tokens.len();
        let tokens_t = Tensor::from_vec(speech_tokens, (1, total_len), &Device::Cpu)?;
        
        let mut inputs = HashMap::new();
        inputs.insert("speech_tokens".to_string(), tokens_t);
        inputs.insert("speaker_embeddings".to_string(), Tensor::zeros((1, 192), candle_core::DType::F32, device)?);
        inputs.insert("speaker_features".to_string(), Tensor::zeros((1, 10, 80), candle_core::DType::F32, device)?);
        
        let outputs = candle_onnx::simple_eval(decoder_model, inputs)?;
        let waveform = outputs.get("waveform").ok_or_else(|| anyhow::anyhow!("No waveform output"))?;
        Ok(waveform.flatten_all()?.to_vec1::<f32>()?)
    }
}

#[derive(Deserialize)]
pub struct SayRequest {
    pub text: String,
}

pub async fn run_speaker_server() -> Result<()> {
    info!("🔊 Starting Integrated Speaker Server...");

    let (_stream, stream_handle) = rodio::OutputStream::try_default()?;
    let speech_sink = Arc::new(rodio::Sink::try_new(&stream_handle)?);
    
    // Silence Carrier
    let carrier_sink = rodio::Sink::try_new(&stream_handle)?;
    let silence = rodio::source::Zero::<f32>::new(1, SAMPLE_RATE_HZ);
    carrier_sink.append(silence);
    carrier_sink.set_volume(0.0);
    carrier_sink.play();
    
    let engine = Arc::new(AudioEngine::new(speech_sink)?);

    let app = Router::new()
        .route("/say", post(say_handler))
        .route("/health", get(|| async { "OK" }))
        .with_state(engine);

    let port = env::var("AGENCY_SPEAKER_PORT").unwrap_or_else(|_| "3000".to_string());
    let addr = format!("0.0.0.0:{}", port);
    info!("🚀 Speaker Server listening at http://{}", addr);
    
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn say_handler(
    State(engine): State<Arc<AudioEngine>>,
    Json(payload): Json<SayRequest>,
) -> Json<serde_json::Value> {
    debug!("Request: {}", payload.text);
    match engine.synthesize(payload.text).await {
        Ok(_) => Json(serde_json::json!({ "status": "ok" })),
        Err(e) => {
            error!("Synthesis failed: {}", e);
            Json(serde_json::json!({ "status": "error", "message": e.to_string() }))
        }
    }
}
