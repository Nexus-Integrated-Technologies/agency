use rust_agency::tools::{Tool, WasmCompilerTool};
use serde_json::json;
use std::sync::Arc;
use futures::future::join_all;

#[tokio::test]
async fn test_concurrent_compilation_load() {
    let compiler = Arc::new(WasmCompilerTool::new());
    
    let mut tasks = Vec::new();
    
    // Simulate 5 concurrent compilation requests
    for i in 0..5 {
        let compiler = compiler.clone();
        tasks.push(tokio::spawn(async move {
            let source = format!(
                r#"
                #[no_mangle]
                pub extern "C" fn add_{}(a: i32, b: i32) -> i32 {{
                    a + b + {}
                }}
                "#, i, i
            );
            
            let params = json!({
                "source_code": source,
                "filename": format!("load_test_module_{}", i)
            });
            
            compiler.execute(params).await
        }));
    }
    
    let results = join_all(tasks).await;
    
    for res in results {
        match res {
            Ok(Ok(tool_output)) => {
                // If success, great. If failure due to missing rustc/target, that's fine for CI enviros,
                // but we check that it didn't PANIC.
                if tool_output.success {
                    assert!(tool_output.data["wasm_path"].as_str().is_some());
                }
            },
            Ok(Err(e)) => panic!("Tool execution error: {}", e),
            Err(e) => panic!("Task join error: {}", e),
        }
    }
}
