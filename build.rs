fn main() {
    if std::env::var_os("CARGO_FEATURE_LEGACY_AGENCY").is_some() {
        println!(
            "cargo:warning=legacy-agency build hooks are disabled during the NanoClaw foundation cutover"
        );
    }
}
