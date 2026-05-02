#!/usr/bin/env node

import crypto from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";

const STATE_ROOT =
  process.env.NANOCLAW_OMX_STATE_ROOT || path.join(os.homedir(), ".nanoclaw-omx");
const POLL_INTERVAL_MS = parseInteger(process.env.NANOCLAW_OMX_POLL_MS, 5_000);
const IDLE_THRESHOLD_MS = parseInteger(process.env.NANOCLAW_OMX_IDLE_MS, 120_000);
const TEAM_MONITOR_TIMEOUT_MS = parseInteger(
  process.env.NANOCLAW_OMX_TEAM_MONITOR_TIMEOUT_MS,
  540_000
);
const EXEC_MONITOR_TIMEOUT_MS = parseInteger(
  process.env.NANOCLAW_OMX_EXEC_MONITOR_TIMEOUT_MS,
  300_000
);
const STATE_READ_RETRIES = parseInteger(process.env.NANOCLAW_OMX_STATE_READ_RETRIES, 5);
const STATE_READ_RETRY_DELAY_MS = parseInteger(
  process.env.NANOCLAW_OMX_STATE_READ_RETRY_DELAY_MS,
  25
);

const [subcommand, ...rest] = process.argv.slice(2);
const wantsJson = rest.includes("--json");

main().catch((error) => {
  if (wantsJson) {
    writeJson({
      ok: false,
      session_id: "",
      status: "failed",
      summary: error.message,
      question: null,
      tmux_session: null,
      team_name: null,
      log_path: null,
      log_body: null,
      artifacts: [],
      external_run_id: null,
      mode: "exec",
      workspace_root: "",
    });
    process.exit(1);
  }
  console.error(error);
  process.exit(1);
});

async function main() {
  if (!subcommand) {
    throw new Error("missing subcommand");
  }

  if (subcommand === "watch") {
    const sessionId = rest[0];
    if (!sessionId) {
      throw new Error("missing session id for watch");
    }
    await watchSession(sessionId);
    return;
  }

  if (subcommand === "finalize") {
    const sessionId = rest[0];
    const exitCode = Number.parseInt(rest[1] || "0", 10);
    if (!sessionId) {
      throw new Error("missing session id for finalize");
    }
    await finalizeSession(sessionId, Number.isNaN(exitCode) ? 1 : exitCode);
    return;
  }

  const request = await readJsonStdin();
  switch (subcommand) {
    case "invoke":
      writeJson(await invokeSession(request));
      return;
    case "status":
      writeJson(await sessionStatus(request.session_id));
      return;
    case "stop":
      writeJson(await stopSession(request.session_id));
      return;
    case "inject":
      writeJson(await injectReply(request.session_id, request.reply));
      return;
    default:
      throw new Error(`unsupported subcommand: ${subcommand}`);
  }
}

async function invokeSession(request) {
  ensureCommand("tmux");
  ensureCommand("omx");

  const mode = normalizeMode(request.mode || "team");
  const cwd = resolveCwd(request.cwd);
  const prompt = String(request.prompt || "").trim();
  const sessionId = String(request.session_id || `omx-${crypto.randomUUID()}`);
  if (!request.group_folder || !String(request.group_folder).trim()) {
    throw new Error("missing group_folder");
  }
  if (!prompt) {
    throw new Error("missing prompt");
  }

  const sessionDir = path.join(STATE_ROOT, "sessions", sessionId);
  await fsp.mkdir(sessionDir, { recursive: true });
  const logPath = path.join(sessionDir, "session.log");
  const runScriptPath = path.join(sessionDir, "run.sh");
  const statePath = path.join(sessionDir, "state.json");
  const tmuxSession = `omx-${compactSuffix(sessionId)}`;
  const now = new Date().toISOString();

  const existing = await maybeReadState(sessionId);
  if (existing && tmuxHasSession(existing.tmux_session)) {
    return buildResponse(existing, {
      summary: existing.summary || "OMX session already running.",
    });
  }

  const state = {
    session_id: sessionId,
    group_folder: String(request.group_folder),
    chat_jid: trimOrNull(request.chat_jid),
    task_id: trimOrNull(request.task_id),
    external_run_id: trimOrNull(request.external_run_id),
    mode,
    max_workers: normalizeWorkers(request.max_workers),
    prompt,
    cwd,
    callback_url: trimOrNull(request.callback_url),
    callback_token: trimOrNull(request.callback_token),
    status: "running",
    tmux_session: tmuxSession,
    team_name: null,
    question: null,
    summary: `OMX ${mode} session launched for ${request.group_folder}.`,
    workspace_root: cwd,
    created_at: now,
    updated_at: now,
    completed_at: null,
    idle_notified_at: null,
    last_output_at: now,
    log_path: logPath,
  };

  await fsp.writeFile(runScriptPath, buildRunScript(state), { mode: 0o755 });
  await writeState(statePath, state);
  await fsp.appendFile(logPath, `[runner] invoke ${now}\n`);

  ensureNoTmuxSession(tmuxSession);
  runOrThrow("tmux", ["new-session", "-d", "-s", tmuxSession, runScriptPath], {
    cwd,
  });
  spawn(process.execPath, [path.resolve(process.argv[1]), "watch", sessionId], {
    detached: true,
    stdio: "ignore",
  }).unref();

  await maybeRefreshTeamName(state);
  await writeState(statePath, state);
  await emitCallback(state, "session-start");
  if (state.mode !== "team") {
    return buildResponse(await waitForNonTeamTerminalState(sessionId));
  }
  return buildResponse(state);
}

async function waitForNonTeamTerminalState(sessionId) {
  const deadline = Date.now() + EXEC_MONITOR_TIMEOUT_MS;
  const statePath = stateFile(sessionId);
  let state = await readState(sessionId);

  while (Date.now() < deadline) {
    await refreshRuntimeState(state);
    state = await readState(sessionId);
    if (isTerminalStatus(state.status)) {
      return state;
    }
    await sleep(POLL_INTERVAL_MS);
  }

  state = await readState(sessionId);
  if (!isTerminalStatus(state.status)) {
    killTmuxSession(state.tmux_session);
    const now = new Date().toISOString();
    const detectedFailure = detectKnownFailureSummary(state.log_path);
    state.status = "failed";
    state.question = null;
    state.summary =
      detectedFailure ||
      `OMX ${state.mode} session did not finish within ${EXEC_MONITOR_TIMEOUT_MS}ms.`;
    state.updated_at = now;
    state.completed_at = now;
    if (state.log_path) {
      await fsp.appendFile(state.log_path, `\n[omx-runner-timeout] ${state.summary}\n`);
    }
    await writeState(statePath, state);
    await emitCallback(state, "session-end");
  }

  return state;
}

async function sessionStatus(sessionId) {
  const state = await readState(sessionId);
  await refreshRuntimeState(state);
  return buildResponse(state, {
    summary: state.summary || summarizeTail(state.log_path, 80),
  });
}

async function stopSession(sessionId) {
  const state = await readState(sessionId);
  await maybeRefreshTeamName(state);
  if (state.team_name) {
    runNoThrow("omx", ["team", "shutdown", state.team_name], { cwd: state.cwd });
  }
  killTmuxSession(state.tmux_session);
  state.status = "stopped";
  state.question = null;
  state.summary = "OMX session stopped.";
  state.updated_at = new Date().toISOString();
  state.completed_at = state.updated_at;
  await writeState(stateFile(sessionId), state);
  await emitCallback(state, "session-stop");
  return buildResponse(state);
}

async function injectReply(sessionId, replyText) {
  const state = await readState(sessionId);
  if (!String(replyText || "").trim()) {
    throw new Error("missing reply");
  }
  if (!tmuxHasSession(state.tmux_session)) {
    throw new Error("OMX tmux session is not running");
  }
  const target = `${state.tmux_session}:0.0`;
  runOrThrow("tmux", ["load-buffer", "-"], { input: `${replyText}` });
  runOrThrow("tmux", ["paste-buffer", "-t", target]);
  runOrThrow("tmux", ["send-keys", "-t", target, "Enter"]);
  state.status = "running";
  state.question = null;
  state.summary = "Reply injected into OMX session.";
  state.updated_at = new Date().toISOString();
  state.last_output_at = state.updated_at;
  await writeState(stateFile(sessionId), state);
  return buildResponse(state);
}

async function watchSession(sessionId) {
  const statePath = stateFile(sessionId);
  let state = await maybeReadState(sessionId);
  if (!state) {
    return;
  }

  let lastSize = fileSize(state.log_path);
  let lastActivity = new Date(state.last_output_at || state.updated_at || state.created_at).getTime();
  while (true) {
    state = await maybeReadState(sessionId);
    if (!state) {
      return;
    }
    if (state.completed_at || isTerminalStatus(state.status)) {
      return;
    }

    await maybeRefreshTeamName(state);
    const missingTeamTerminal = await maybeFinalizeMissingTeam(state);
    if (missingTeamTerminal) {
      await writeState(statePath, state);
      await emitCallback(state, "session-end");
      return;
    }

    const teamTerminal = await maybeRefreshTeamStatus(state);
    if (teamTerminal) {
      await writeState(statePath, state);
      await emitCallback(state, "session-end");
      return;
    }

    const alive = tmuxHasSession(state.tmux_session);
    const currentSize = fileSize(state.log_path);
    if (currentSize > lastSize) {
      const chunk = await readRange(state.log_path, lastSize);
      lastSize = currentSize;
      lastActivity = Date.now();
      state.last_output_at = new Date(lastActivity).toISOString();
      const question = detectQuestion(chunk);
      if (question && question !== state.question) {
        state.question = question;
        state.status = "waiting_input";
        state.summary = question;
        state.updated_at = new Date().toISOString();
        await writeState(statePath, state);
        await emitCallback(state, "ask-user-question");
      } else {
        state.status = "running";
        state.updated_at = new Date().toISOString();
        await writeState(statePath, state);
      }
    } else if (alive && Date.now() - lastActivity >= IDLE_THRESHOLD_MS) {
      const lastIdle = state.idle_notified_at ? Date.parse(state.idle_notified_at) : 0;
      if (!lastIdle || Date.now() - lastIdle >= IDLE_THRESHOLD_MS) {
        state.status = "idle";
        state.summary = summarizeTail(state.log_path, 40) || "OMX session is idle.";
        state.updated_at = new Date().toISOString();
        state.idle_notified_at = state.updated_at;
        await writeState(statePath, state);
        await emitCallback(state, "session-idle");
      }
    }

    if (!alive && state.mode !== "team") {
      return;
    }
    await sleep(POLL_INTERVAL_MS);
  }
}

async function finalizeSession(sessionId, exitCode) {
  const state = await maybeReadState(sessionId);
  if (!state || state.completed_at) {
    return;
  }
  await maybeRefreshTeamName(state);
  const now = new Date().toISOString();
  if (state.status === "stopped") {
    state.summary = state.summary || "OMX session stopped.";
  } else if (exitCode === 0) {
    state.status = "completed";
    state.summary = summarizeTail(state.log_path, 80) || "OMX session completed.";
  } else {
    state.status = "failed";
    state.summary =
      detectKnownFailureSummary(state.log_path) ||
      summarizeTail(state.log_path, 80) || `OMX session failed with exit code ${exitCode}.`;
  }
  state.updated_at = now;
  state.completed_at = now;
  await writeState(stateFile(sessionId), state);
  await emitCallback(state, state.status === "stopped" ? "session-stop" : "session-end");
}

function buildRunScript(state) {
  const runnerPath = path.resolve(process.argv[1]);
  const omxCommand = buildOmxCommand(state);
  const logPath = shellQuote(state.log_path);
  const shouldFinalizeInScript = state.mode !== "team";
  const lines = [
    "#!/usr/bin/env bash",
    "set +e",
    'export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:${PATH:-}"',
    `cd ${shellQuote(state.cwd)}`,
    `${omxCommand} >> ${logPath} 2>&1`,
    "status=$?",
    `printf '\\n[omx-runner-exit] %s\\n' \"$status\" >> ${logPath}`,
    ...(shouldFinalizeInScript
      ? [
          `${shellQuote(process.execPath)} ${shellQuote(runnerPath)} finalize ${shellQuote(
            state.session_id
          )} \"$status\"`,
        ]
      : []),
    "exit \"$status\"",
  ];
  return `${lines.join("\n")}\n`;
}

function buildOmxCommand(state) {
  const prompt = shellQuote(state.prompt);
  const codexExecPrefix = buildCodexExecPrefix(state.cwd);
  switch (state.mode) {
    case "exec":
      return `${codexExecPrefix} ${prompt}`;
    case "team":
      return `omx team ${state.max_workers || 3}:executor ${prompt}`;
    case "ralph":
      return `omx ralph ${prompt}`;
    case "deep-interview":
      return `${codexExecPrefix} ${shellQuote(`$deep-interview ${state.prompt}`)}`;
    case "ralplan":
      return `${codexExecPrefix} ${shellQuote(`$ralplan ${state.prompt}`)}`;
    default:
      return `${codexExecPrefix} ${prompt}`;
  }
}

function buildCodexExecPrefix(cwd) {
  return [
    "omx",
    "exec",
    ...codexExecSandboxArgs(),
    "--json",
    "--skip-git-repo-check",
    "--cd",
    cwd,
  ]
    .map(shellQuote)
    .join(" ");
}

function codexExecSandboxArgs() {
  const sandbox = String(
    process.env.NANOCLAW_CODEX_SANDBOX || process.env.CODEX_SANDBOX || ""
  ).trim();
  if (!sandbox) return [];
  if (sandbox === "danger-full-access") {
    return ["--dangerously-bypass-approvals-and-sandbox"];
  }
  return ["-s", sandbox];
}

async function refreshRuntimeState(state) {
  await maybeRefreshTeamName(state);
  const missingTeamTerminal = await maybeFinalizeMissingTeam(state);
  if (missingTeamTerminal) {
    await writeState(stateFile(state.session_id), state);
    return;
  }

  const teamTerminal = await maybeRefreshTeamStatus(state);
  if (teamTerminal) {
    await writeState(stateFile(state.session_id), state);
    return;
  }
  if (
    state.mode !== "team" &&
    !tmuxHasSession(state.tmux_session) &&
    !isTerminalStatus(state.status)
  ) {
    const exitCode = readExitCode(state.log_path);
    await finalizeSession(state.session_id, exitCode ?? 0);
    const refreshed = await readState(state.session_id);
    Object.assign(state, refreshed);
    return;
  }
  if (state.mode === "team" && state.team_name) {
    const result = runNoThrow(
      "omx",
      ["team", "status", state.team_name, "--json"],
      { cwd: state.cwd, encoding: "utf8" }
    );
    if (result.ok && result.stdout.trim()) {
      state.summary = summarizeJsonStatus(result.stdout, state.summary);
      state.team_status_json = result.stdout.trim();
    }
  } else {
    state.summary = summarizeTail(state.log_path, 80) || state.summary;
  }
  state.updated_at = new Date().toISOString();
  await writeState(stateFile(state.session_id), state);
}

async function maybeRefreshTeamName(state) {
  if (state.mode !== "team" || state.team_name) {
    return;
  }
  const fromLog = detectTeamNameFromLog(state.log_path);
  if (fromLog) {
    state.team_name = fromLog;
    return;
  }
  const detected = await detectTeamName(state.cwd, state.created_at);
  if (detected) {
    state.team_name = detected;
  }
}

async function maybeFinalizeMissingTeam(state) {
  if (
    state.mode !== "team" ||
    state.team_name ||
    isTerminalStatus(state.status) ||
    tmuxHasSession(state.tmux_session)
  ) {
    return false;
  }

  const exitCode = readExitCode(state.log_path);
  state.status = "failed";
  state.summary =
    exitCode === null
      ? "OMX team launcher exited before a team was available."
      : `OMX team launcher exited with status ${exitCode} before a team was available.`;
  state.question = null;
  state.updated_at = new Date().toISOString();
  state.completed_at = state.updated_at;
  return true;
}

async function maybeRefreshTeamStatus(state) {
  if (state.mode !== "team" || !state.team_name || isTerminalStatus(state.status)) {
    return false;
  }

  const result = runNoThrow(
    "omx",
    ["team", "status", state.team_name, "--json"],
    { cwd: state.cwd, encoding: "utf8" }
  );
  if (result.ok && result.stdout.trim()) {
    state.team_status_json = result.stdout.trim();
    state.summary = summarizeJsonStatus(result.stdout, state.summary);
    const status = parseTeamStatus(result.stdout);
    if (status.terminal) {
      state.status = status.failed ? "failed" : "completed";
      state.summary = status.summary || state.summary || "OMX team completed.";
      state.question = null;
      state.updated_at = new Date().toISOString();
      state.completed_at = state.updated_at;
      return true;
    }
  }

  if (Date.now() - (Date.parse(state.created_at) || Date.now()) >= TEAM_MONITOR_TIMEOUT_MS) {
    state.status = "failed";
    state.summary = `OMX team did not finish within ${TEAM_MONITOR_TIMEOUT_MS}ms.`;
    state.updated_at = new Date().toISOString();
    state.completed_at = state.updated_at;
    return true;
  }

  return false;
}

function parseTeamStatus(raw) {
  try {
    const parsed = parseJsonValue(raw);
    const tasks = parsed && typeof parsed === "object" ? parsed.tasks : null;
    if (!tasks || typeof tasks !== "object") {
      return { terminal: false, failed: false, summary: null };
    }
    const total = numberOrZero(tasks.total);
    const pending = numberOrZero(tasks.pending);
    const blocked = numberOrZero(tasks.blocked);
    const inProgress = numberOrZero(tasks.in_progress);
    const failed = numberOrZero(tasks.failed);
    const completed = numberOrZero(tasks.completed);
    if (total > 0 && pending + blocked + inProgress === 0 && completed + failed >= total) {
      return {
        terminal: true,
        failed: failed > 0,
        summary:
          failed > 0
            ? `OMX team finished with ${failed} failed task(s).`
            : `OMX team completed ${completed} task(s).`,
      };
    }
  } catch {
    return { terminal: false, failed: false, summary: null };
  }
  return { terminal: false, failed: false, summary: null };
}

function numberOrZero(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function detectTeamNameFromLog(filePath) {
  const match = safeReadFile(filePath).match(/^Team started:\s*(.+)$/m);
  return match ? match[1].trim() : null;
}

async function detectTeamName(cwd, createdAt) {
  const teamRoot = path.join(cwd, ".omx", "state", "team");
  let entries;
  try {
    entries = await fsp.readdir(teamRoot, { withFileTypes: true });
  } catch {
    return null;
  }
  const createdTs = Date.parse(createdAt) || 0;
  let best = null;
  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }
    const fullPath = path.join(teamRoot, entry.name);
    const stat = await fsp.stat(fullPath);
    if (stat.mtimeMs + 60_000 < createdTs) {
      continue;
    }
    if (!best || stat.mtimeMs > best.mtimeMs) {
      best = { name: entry.name, mtimeMs: stat.mtimeMs };
    }
  }
  return best?.name || null;
}

async function emitCallback(state, event) {
  if (!state.callback_url) {
    return;
  }
  const payload = {
    token: state.callback_token || null,
    event,
    session_id: state.session_id,
    group_folder: state.group_folder,
    chat_jid: state.chat_jid || null,
    task_id: state.task_id || null,
    external_run_id: state.external_run_id || null,
    mode: state.mode,
    status: state.status,
    tmux_session: state.tmux_session || null,
    team_name: state.team_name || null,
    summary: state.summary || null,
    question: state.question || null,
    workspace_root: state.workspace_root,
    artifacts: buildArtifacts(state),
    created_at: new Date().toISOString(),
  };
  const headers = { "content-type": "application/json" };
  if (state.callback_token) {
    headers.authorization = `Bearer ${state.callback_token}`;
    headers["x-nanoclaw-omx-token"] = state.callback_token;
  }
  try {
    const response = await fetch(state.callback_url, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      let responseBody = "";
      try {
        responseBody = await response.text();
      } catch {
        responseBody = "";
      }
      const details = responseBody.trim();
      await fsp.appendFile(
        state.log_path,
        `\n[runner-callback-http-error] status=${response.status} status_text=${response.statusText}${
          details ? ` body=${details}` : ""
        }\n`
      );
    }
  } catch (error) {
    await fsp.appendFile(
      state.log_path,
      `\n[runner-callback-error] ${String(error?.message || error)}\n`
    );
  }
}

function buildArtifacts(state) {
  const artifacts = [];
  if (state.log_path) {
    artifacts.push({
      kind: "omx-log",
      title: "OMX session log",
      location: state.log_path,
      body: summarizeTail(state.log_path, 40) || null,
    });
  }
  if (state.team_status_json) {
    artifacts.push({
      kind: "team-status",
      title: "OMX team status",
      location: state.team_name ? `.omx/state/team/${state.team_name}` : null,
      body: state.team_status_json,
    });
  }
  return artifacts;
}

function buildResponse(state, overrides = {}) {
  return {
    ok: true,
    session_id: state.session_id,
    status: state.status,
    tmux_session: state.tmux_session || null,
    team_name: state.team_name || null,
    summary: overrides.summary || state.summary || "",
    question: overrides.question ?? state.question ?? null,
    log_path: state.log_path || null,
    log_body: summarizeTail(state.log_path, 80) || null,
    artifacts: buildArtifacts(state),
    external_run_id: state.external_run_id || null,
    mode: state.mode,
    workspace_root: state.workspace_root,
  };
}

async function readJsonStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) {
    chunks.push(Buffer.from(chunk));
  }
  const raw = Buffer.concat(chunks).toString("utf8").trim();
  if (!raw) {
    return {};
  }
  return JSON.parse(raw);
}

async function readState(sessionId) {
  const state = await maybeReadState(sessionId);
  if (!state) {
    throw new Error(`OMX session not found: ${sessionId}`);
  }
  return state;
}

async function maybeReadState(sessionId) {
  const filePath = stateFile(sessionId);
  let lastError = null;
  for (let attempt = 0; attempt <= STATE_READ_RETRIES; attempt += 1) {
    try {
      const raw = await fsp.readFile(filePath, "utf8");
      return JSON.parse(raw);
    } catch (error) {
      if (error && error.code === "ENOENT") {
        return null;
      }
      lastError = error;
      if (!isJsonParseError(error) || attempt === STATE_READ_RETRIES) {
        break;
      }
      await sleep(STATE_READ_RETRY_DELAY_MS);
    }
  }
  throw lastError;
}

async function writeState(filePath, state) {
  const dir = path.dirname(filePath);
  await fsp.mkdir(dir, { recursive: true });
  const tmpPath = path.join(
    dir,
    `.state.${process.pid}.${Date.now()}.${Math.random().toString(16).slice(2)}.tmp`
  );
  await fsp.writeFile(tmpPath, `${JSON.stringify(state, null, 2)}\n`);
  await fsp.rename(tmpPath, filePath);
}

function stateFile(sessionId) {
  return path.join(STATE_ROOT, "sessions", sessionId, "state.json");
}

function summarizeTail(filePath, lines) {
  const content = safeReadFile(filePath);
  if (!content.trim()) {
    return "";
  }
  return content.trim().split(/\r?\n/).slice(-lines).join("\n").slice(-8_000);
}

function detectKnownFailureSummary(filePath) {
  const content = safeReadFile(filePath);
  if (!content.trim()) {
    return null;
  }
  if (
    /401 Unauthorized/i.test(content) &&
    /Missing bearer or basic authentication/i.test(content)
  ) {
    return "Codex auth failed: OpenAI returned 401 Unauthorized. Refresh PAPERCLIP_CODEX_AUTH_JSON or PAPERCLIP_CODEX_AUTH_JSON_B64 for the gateway container.";
  }
  if (/required command not found:\s*omx/i.test(content) || /command not found:\s*omx/i.test(content)) {
    return "OMX runner failed: the omx command is not installed or not on PATH in the gateway container.";
  }
  return null;
}

function summarizeJsonStatus(raw, fallback) {
  try {
    const parsed = parseJsonValue(raw);
    return (
      parsed.summary ||
      parsed.statusline ||
      parsed.status ||
      fallback ||
      "OMX team status available."
    );
  } catch {
    return fallback || "OMX team status available.";
  }
}

function parseJsonValue(raw) {
  const text = String(raw || "").trim();
  if (!text) {
    throw new SyntaxError("empty JSON payload");
  }
  try {
    return JSON.parse(text);
  } catch (error) {
    const extracted = extractFirstJsonValue(text);
    if (extracted) {
      return JSON.parse(extracted);
    }
    throw error;
  }
}

function extractFirstJsonValue(text) {
  const start = text.search(/[\[{]/);
  if (start < 0) {
    return null;
  }
  const opening = text[start];
  const closing = opening === "{" ? "}" : "]";
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let index = start; index < text.length; index += 1) {
    const char = text[index];
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (char === "\\") {
        escaped = true;
      } else if (char === "\"") {
        inString = false;
      }
      continue;
    }
    if (char === "\"") {
      inString = true;
      continue;
    }
    if (char === opening) {
      depth += 1;
      continue;
    }
    if (char === closing) {
      depth -= 1;
      if (depth === 0) {
        return text.slice(start, index + 1);
      }
    }
  }
  return null;
}

function isJsonParseError(error) {
  return error instanceof SyntaxError;
}

function detectQuestion(text) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    const line = lines[index];
    if (
      line.endsWith("?") ||
      /^question:/i.test(line) ||
      /need(s)? input/i.test(line) ||
      /reply .* to continue/i.test(line)
    ) {
      return line.slice(0, 1_500);
    }
  }
  return null;
}

function readExitCode(filePath) {
  const tail = summarizeTail(filePath, 10);
  const match = tail.match(/\[omx-runner-exit\]\s+(\d+)/);
  if (!match) {
    return null;
  }
  const value = Number.parseInt(match[1], 10);
  return Number.isNaN(value) ? null : value;
}

function tmuxHasSession(sessionName) {
  if (!sessionName) {
    return false;
  }
  const result = spawnSync("tmux", ["has-session", "-t", sessionName], {
    stdio: "ignore",
  });
  return result.status === 0;
}

function ensureNoTmuxSession(sessionName) {
  if (tmuxHasSession(sessionName)) {
    killTmuxSession(sessionName);
  }
}

function killTmuxSession(sessionName) {
  if (!sessionName) {
    return;
  }
  runNoThrow("tmux", ["kill-session", "-t", sessionName]);
}

function runOrThrow(command, args, options = {}) {
  const result = spawnSync(command, args, {
    stdio: options.input ? ["pipe", "pipe", "pipe"] : ["ignore", "pipe", "pipe"],
    encoding: options.encoding || "utf8",
    cwd: options.cwd,
    input: options.input,
  });
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(" ")} failed: ${(result.stderr || "").trim() || result.status}`
    );
  }
  return result;
}

function runNoThrow(command, args, options = {}) {
  const result = spawnSync(command, args, {
    stdio: options.input ? ["pipe", "pipe", "pipe"] : ["ignore", "pipe", "pipe"],
    encoding: options.encoding || "utf8",
    cwd: options.cwd,
    input: options.input,
  });
  return {
    ok: result.status === 0,
    stdout: result.stdout || "",
    stderr: result.stderr || "",
    status: result.status,
  };
}

function ensureCommand(command) {
  const result = spawnSync("sh", ["-lc", `command -v ${command}`], {
    stdio: "ignore",
  });
  if (result.status !== 0) {
    throw new Error(`required command not found: ${command}`);
  }
}

function writeJson(value) {
  process.stdout.write(`${JSON.stringify(value)}\n`);
}

function parseInteger(raw, fallback) {
  const parsed = Number.parseInt(String(raw || ""), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function normalizeWorkers(value) {
  const parsed = Number.parseInt(String(value || ""), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 3;
}

function normalizeMode(value) {
  const normalized = String(value || "team").trim().toLowerCase();
  if (
    ["exec", "team", "ralph", "deep-interview", "ralplan"].includes(normalized)
  ) {
    return normalized;
  }
  return "exec";
}

function resolveCwd(raw) {
  const cwd = path.resolve(String(raw || process.cwd()));
  if (!fs.existsSync(cwd)) {
    throw new Error(`cwd does not exist: ${cwd}`);
  }
  return cwd;
}

function compactSuffix(sessionId) {
  const compact = String(sessionId).replace(/[^a-zA-Z0-9]/g, "");
  return compact.length <= 12 ? compact : compact.slice(-12);
}

function trimOrNull(value) {
  const trimmed = String(value || "").trim();
  return trimmed ? trimmed : null;
}

function isTerminalStatus(status) {
  return ["completed", "failed", "stopped"].includes(String(status || "").trim());
}

function safeReadFile(filePath) {
  try {
    return fs.readFileSync(filePath, "utf8");
  } catch {
    return "";
  }
}

async function readRange(filePath, start) {
  try {
    const handle = await fsp.open(filePath, "r");
    const stat = await handle.stat();
    const size = Math.max(stat.size - start, 0);
    const buffer = Buffer.alloc(size);
    await handle.read(buffer, 0, size, start);
    await handle.close();
    return buffer.toString("utf8");
  } catch {
    return "";
  }
}

function fileSize(filePath) {
  try {
    return fs.statSync(filePath).size;
  } catch {
    return 0;
  }
}

function shellQuote(value) {
  return `'${String(value).replace(/'/g, `'\"'\"'`)}'`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
