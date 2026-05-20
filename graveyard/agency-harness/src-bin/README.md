# Binaries & Servers

Contains the executable entry points for the Agency's distributed micro-services and utility scripts.

## 🚀 Key Servers

- **`speaker_server.rs`**: High-fidelity TTS engine using the T3 transformer and HiFT-GAN vocoder. Optimized for Apple Silicon (MPS).
- **`listener_server.rs`**: Real-time voice-to-nexus gateway using Whisper (SOTA quantized). Features VAD-triggered auto-transcription.
- **`memory_server.rs`**: A vector memory microservice providing Axum-based storage and semantic search endpoints.

## 🛠️ Utilities & Ingestion

- **`ingest_pdf.rs`**: High-speed PDF processing into vector memory with context-aware chunking.
- **`inspect_gguf.rs`**: Utility to dump metadata from GGUF model files.
- **`convert_onnx.rs`**: Validation script for ONNX-exported neural graphs.

## 🧪 Testing

- **`test_paralinguistics.rs`**: Validates the speaker's ability to render emotional/conversational tags like `[laugh]`.
- **`test_ort_simple.rs`**: Baseline verification for ONNX Runtime integration.
