pub fn run_desktop() {
  tauri::Builder::default()
    .setup(|app| {
      Ok(())
    })
    .run(tauri::generate_context!("src-tauri/tauri.conf.json"))
    .expect("error while running tauri application");
}
