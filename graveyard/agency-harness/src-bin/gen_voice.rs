use anyhow::Result;
use rand::Rng;

fn main() -> Result<()> {
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: 24000,
        bits_per_sample: 32,
        sample_format: hound::SampleFormat::Float,
    };
    let mut writer = hound::WavWriter::create("/Users/javoerokour/Desktop/BUDDHA/CODE/agency/rust_agency/artifacts/chatterbox/target_voice.wav", spec)?;
    let mut rng = rand::thread_rng();
    for _ in 0..24000 {
        let val: f32 = rng.gen_range(-0.01..0.01);
        writer.write_sample(val)?;
    }
    println!("✅ Generated target_voice.wav with noise.");
    Ok(())
}