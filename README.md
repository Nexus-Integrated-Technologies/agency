# Agency on a NanoClaw Foundation

This clone is being refocused into a pure-Rust Agency runtime built on a
NanoClaw-style foundation.
The goal is not to make Agency as small as NanoClaw; the goal is to ensure the
runtime, artifacts, and higher-order features descend from the same core domain
model.

- The canonical base layer now lives in `src/foundation/`.
- The first runtime descendant lives in `src/bin/nanoclaw.rs` and `src/nanoclaw/`.
- `cargo run` now defaults to the NanoClaw Rust bootstrap path.
- Legacy Agency modules are no longer on the default compile path; they are
  gated behind the `legacy-agency` Cargo feature during the cutover.
- Holonic and governance artifacts that get pruned are moved into
  `graveyard/holonic/` instead of being deleted.
- The foundation rules are described in `docs/foundation-model.md`.
- The DigitalOcean VM workflow is described in
  `docs/digitalocean-dev-environment.md`.
- The migration map is tracked in `docs/nanoclaw-rs-migration.md`.

> Legacy Agency README content follows below for reference while the cutover is
> still in progress.

# 🚀 SOTA Semi-Autonomous Agency (Rust) v0.2.0

A state-of-the-art, semi-autonomous multi-agent system built in Rust. This agency features a ReAct reasoning framework, distributed microservices architecture, **First Principle Framework (FPF)** integration, and SOTA audio capabilities. It is designed for complex technical tasks, autonomous problem-solving, and seamless human-AI interaction via text and voice.

## ✨ Key Features

- **🧩 Distributed Microservices**: Decomposed into specialized servers for robust scalability:
  - **Nexus Server**: The central orchestrator and brain.
  - **Memory Server**: Dedicated semantic knowledge management.
  - **Speaker Server**: Low-latency, high-fidelity TTS using **Candle** and **ONNX**.
  - **Listener Server**: Whisper-based speech recognition.
- **🧠 ReAct Reasoning Framework**: Implements the Reason+Act paradigm with self-reflection and iterative planning.
- **🧬 First Principle Framework (FPF)**: Adheres to FPF principles for capability scoping (`U.WorkScope`), characteristic aggregation, and multi-view publication.
- **🔌 Model Context Protocol (MCP)**: Native support for connecting external MCP servers to extend tool capabilities dynamically.
- **📚 Semantic Memory**: Integrates **ChromaDB** and **fastembed** for high-performance vector storage and retrieval.
- **🗣️ SOTA Audio Engine**: Features **T3 Turbo** and **Candle** for local, privacy-focused, and high-quality voice synthesis.
- **🛡️ Enterprise Safety**: Process hardening, input validation, and content filtering.
- **🔒 Deep Isolation**: Hybrid security architecture using **macOS Seatbelt** for low-latency host hardening and **Podman** for rootless code execution.
- **🔭 Observability**: Built-in **OpenTelemetry** tracing for deep system introspection.
- **🛠️ Extensible Tool System**: Dynamic tool loading, **Forge** for creating tools on-the-fly, and Markdown-based **Skill Discovery**.

## 🏗️ Architecture

The system operates as a constellation of microservices managed by the `start_agency.sh` script:

### 1. Nexus Server (`src/bin/nexus_server.rs`)
The orchestrator. It manages the agent lifecycle, executes the ReAct loop, handles tool calls, and routes tasks to specialized agents. It integrates with **Ollama** or local **Candle** models for inference.

### 2. Speaker Server (`src/bin/speaker_server.rs`)
The "Mouth" of the agency. A dedicated server running a custom T3 transformer pipeline via Candle/ONNX for rapid, natural-sounding speech synthesis.

### 3. Memory Server (`src/bin/memory_server.rs`)
The "Hippocampus". Manages long-term storage, vector embeddings, and retrieval operations, ensuring the agency retains context across sessions.

### 4. Listener Server (`src/bin/listener_server.rs`)
The "Ears". Runs a Whisper model to transcribe audio input into text for the Nexus server.

## 🛠️ Tools & Capabilities

The agency comes with a powerful registry of tools (`src/tools/`):

- **`web_search`**: Live internet data retrieval.
- **`code_exec`**: Secure, sandboxed code execution.
- **`codebase`**: Semantic analysis and navigation of local project files.
- **`memory_query`**: Deep retrieval from the agency's vector store.
- **`knowledge_graph`**: structured data relationship management.
- **`visualization`**: Generates system visualizations (e.g., isometric architecture views).
- **`science`**: specialized scientific calculation and data analysis tools.
- **`speaker_rs`**: Direct interface to the Speaker Server.
- **`forge`**: Meta-tool for creating new custom tools during runtime.
- **`mcp`**: Proxy tools for connected MCP servers.

## 🚀 Getting Started

### Prerequisites
- **Rust Toolchain**: [Install Rust](https://www.rust-lang.org/tools/install) (1.75+).
- **Podman**: Required for sandboxed code execution and infrastructure. (`brew install podman podman-compose`)
- **Python 3.10+**: (Optional) For some utility scripts and ONNX exports.
- **Ollama** or **Local Models**: Ensure you have an LLM backend available (Llama 3, Mistral, etc.).

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/ProdByBuddha/rust_agency.git
    cd rust_agency
    ```

2.  **Environment Setup:**
    Create a `.env` file in the root directory:
    ```bash
    # Core
    RUST_LOG=info
    AGENCY_PROFILE=agency_profile.json
    
    # LLM Provider
    OLLAMA_HOST=http://localhost:11434
    
    # Services Config
    AGENCY_SPEAKER_PORT=3000
    AGENCY_MEMORY_PORT=3001
    
    # Features
    AGENCY_ENABLE_MOUTH=1  # Enable Speaker
    AGENCY_ENABLE_EARS=0   # Enable Listener
    ```

3.  **Models & Artifacts:**
    Ensure required model artifacts (ONNX/Safetensors) are placed in `artifacts/chatterbox/` for the Speaker system.

### Running the Agency

The recommended way to start the full system (orchestrator + microservices) is via the startup script:

```bash
./start_agency.sh
```

This script will:
1. Build all necessary binaries (`nexus_server`, `speaker_server`, etc.).
2. Launch enabled microservices in the background.
3. Wait for health checks to pass.
4. Start the interactive Nexus CLI.

### CLI Commands
Once inside the Nexus CLI:
- **`autonomous`**: Enter autonomous goal-seeking mode.
- **`visualize`**: Generate a visualization of the current system state.
- **`clear`**: Reset session context.
- **`quit`**: Save state, shutdown services, and exit.

## 🔧 Configuration

- **`agency_profile.json`**: Define the agent's persona, mission, and traits.
- **`mcp_servers.json`**: Register external MCP servers to extend capabilities.
  ```json
  {
    "servers": [
      {
        "name": "filesystem",
        "command": "npx",
        "args": ["-y", "@modelcontextprotocol/server-filesystem", "/path/to/allow"]
      }
    ]
  }
  ```
- **`skills/`**: Add Markdown files here to teach the agency new static procedures.

## 🤝 Contributing

Contributions are welcome! Please follow the **FPF** guidelines when adding new capabilities.

## 📄 License

Licensed under the **Functional Source License (FSL) 1.1**. 

- **Permissions**: You can view, modify, and run this software for any purpose.
- **Restriction**: You may **not** use this software to build a competing product or service.
- **Conversion**: On **2028-01-16**, this version automatically converts to **Apache 2.0**.
- **Contribution**: Contributors are required to agree to the [Contributor License Agreement (CLA)](CLA.md).

## Star History

<a href="https://www.star-history.com/#prodbybuddha/agency&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=prodbybuddha/agency&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=prodbybuddha/agency&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=prodbybuddha/agency&type=date&legend=top-left" />
 </picture>
</a>
