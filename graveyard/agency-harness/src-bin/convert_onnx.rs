use anyhow::{Result, Context};
use candle_core::{Device, Tensor};
use std::collections::HashMap;

fn main() -> Result<()> {
    let path = "/Users/javoerokour/Desktop/BUDDHA/CODE/agency/rust_agency/artifacts/chatterbox/conditional_decoder_q8_full.onnx";
    let model = candle_onnx::read_file(path)?;
    let device = Device::Cpu;

    let mut inputs = HashMap::new();
    inputs.insert("speech_tokens".to_string(), Tensor::zeros((1, 20), candle_core::DType::I64, &device)?);
    inputs.insert("speaker_embeddings".to_string(), Tensor::zeros((1, 192), candle_core::DType::F32, &device)?);
    inputs.insert("speaker_features".to_string(), Tensor::zeros((1, 1, 80), candle_core::DType::F32, &device)?);

    println!("Running simple_eval on the model...");
    // simple_eval executes the entire graph
    let _outputs = candle_onnx::eval::simple_eval(&model, inputs)
        .context("Execution failed in simple_eval")?;

    println!("Success!");
    Ok(())
}
