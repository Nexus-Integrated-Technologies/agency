use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::foundation::{
    Group, RequestPlane, SwarmRequestedLane, SwarmResolvedLane, SwarmRun, SwarmRunStatus,
    SwarmTask, SwarmTaskDependency, SwarmTaskStatus,
};

use super::app::NanoclawApp;
use super::db::NanoclawDb;
use super::dev_environment::DigitalOceanDevEnvironment;
use super::executor::{
    build_execution_session, ExecutionRequest, ExecutionResponse, ExecutorBoundary,
};
use super::model_router::{is_command_available, WorkerBackend};
use super::request_plane::{
    command_violates_request_plane, detect_request_plane_requirements,
    get_request_plane_text_error, normalize_request_plane,
};

const DEFAULT_SWARM_MAX_STEPS: usize = 8;
const DEFAULT_SWARM_MAX_CONCURRENCY: i64 = 2;
const DEFAULT_SWARM_MAX_ATTEMPTS: i64 = 3;
const DEFAULT_SWARM_LEASE_MS: i64 = 5 * 60 * 1000;
const RESULT_SUMMARY_LIMIT: usize = 240;
const RESULT_OUTPUT_LIMIT: usize = 8000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SwarmPlanTaskInput {
    pub key: Option<String>,
    pub title: String,
    pub prompt: Option<String>,
    pub command: Option<String>,
    pub lane: Option<SwarmRequestedLane>,
    pub priority: Option<i64>,
    pub target_group_folder: Option<String>,
    pub cwd: Option<String>,
    pub repo: Option<String>,
    pub repo_path: Option<String>,
    pub sync: Option<bool>,
    pub host: Option<String>,
    pub user: Option<String>,
    pub port: Option<u16>,
    pub timeout_ms: Option<u64>,
    pub request_plane: Option<RequestPlane>,
    pub metadata: Option<Value>,
    pub max_attempts: Option<i64>,
    pub depends_on: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateSwarmObjectiveRunInput {
    pub objective: String,
    pub group_folder: String,
    pub chat_jid: String,
    pub created_by: String,
    pub requested_lane: Option<SwarmRequestedLane>,
    pub tasks: Vec<SwarmPlanTaskInput>,
    pub max_concurrency: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SwarmRunDetails {
    pub run: SwarmRun,
    pub tasks: Vec<SwarmTask>,
    pub dependencies: Vec<SwarmTaskDependency>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SwarmPumpSummary {
    pub expired_leases_released: usize,
    pub runs_considered: usize,
    pub tasks_claimed: usize,
    pub tasks_completed: usize,
    pub tasks_failed: usize,
}

#[derive(Debug, Clone)]
struct RunnableSwarmTask {
    run: SwarmRun,
    task: SwarmTask,
    dependencies: Vec<SwarmTask>,
    resolved_lane: SwarmResolvedLane,
}

#[derive(Debug, Clone)]
struct SwarmTaskExecutionResult {
    ok: bool,
    summary: String,
    output: Option<String>,
    error: Option<String>,
    metadata: Option<Value>,
    non_retryable: bool,
}

pub fn create_swarm_objective_run(
    app: &mut NanoclawApp,
    input: CreateSwarmObjectiveRunInput,
) -> Result<SwarmRunDetails> {
    let run_id = format!("swarm-run-{}", Uuid::new_v4());
    let now = Utc::now().to_rfc3339();
    let requested_lane = input
        .requested_lane
        .clone()
        .unwrap_or(SwarmRequestedLane::Auto);
    let max_concurrency = input
        .max_concurrency
        .unwrap_or(DEFAULT_SWARM_MAX_CONCURRENCY)
        .max(1);
    let (tasks, dependencies) = build_swarm_plan(&input, &run_id, &now)?;
    let run = SwarmRun {
        id: run_id.clone(),
        group_folder: input.group_folder,
        chat_jid: input.chat_jid,
        created_by: input.created_by,
        objective: input.objective,
        requested_lane,
        status: SwarmRunStatus::Pending,
        max_concurrency,
        summary: None,
        result: None,
        created_at: now.clone(),
        updated_at: now,
        completed_at: None,
    };
    app.db.create_swarm_run(&run, &tasks, &dependencies)?;
    get_swarm_run_details(&app.db, &run_id)?.with_context(|| {
        format!(
            "swarm run {} disappeared immediately after creation",
            run_id
        )
    })
}

pub fn get_swarm_run_details(db: &NanoclawDb, run_id: &str) -> Result<Option<SwarmRunDetails>> {
    let Some(run) = db.get_swarm_run(run_id)? else {
        return Ok(None);
    };
    Ok(Some(SwarmRunDetails {
        tasks: db.list_swarm_tasks(run_id)?,
        dependencies: db.list_swarm_task_dependencies(run_id)?,
        run,
    }))
}

pub fn list_swarm_run_details(db: &NanoclawDb, limit: usize) -> Result<Vec<SwarmRunDetails>> {
    let runs = db.list_swarm_runs(limit)?;
    let mut details = Vec::with_capacity(runs.len());
    for run in runs {
        details.push(SwarmRunDetails {
            tasks: db.list_swarm_tasks(&run.id)?,
            dependencies: db.list_swarm_task_dependencies(&run.id)?,
            run,
        });
    }
    Ok(details)
}

pub fn cancel_swarm_objective_run(app: &NanoclawApp, run_id: &str) -> Result<()> {
    app.db.cancel_swarm_run(run_id, &Utc::now().to_rfc3339())
}

pub fn pump_swarm_once<E: ExecutorBoundary>(
    app: &mut NanoclawApp,
    executor: &E,
) -> Result<SwarmPumpSummary> {
    let mut summary = SwarmPumpSummary::default();
    let mut runs_considered = HashSet::new();
    let now = Utc::now().to_rfc3339();
    summary.expired_leases_released = app.db.release_expired_swarm_task_leases(&now)?;
    let max_steps = swarm_max_steps();

    for _ in 0..max_steps {
        let runs = app.db.list_active_swarm_runs()?;
        runs_considered.extend(runs.iter().map(|run| run.id.clone()));
        if runs.is_empty() {
            break;
        }
        let runnable = build_runnable_tasks(app, &runs)?;
        let Some(candidate) = runnable.into_iter().next() else {
            break;
        };

        let started_at = Utc::now().to_rfc3339();
        let lease_owner = format!("swarm:{}:{}", std::process::id(), candidate.task.id);
        let lease_expires_at = chrono::DateTime::parse_from_rfc3339(&started_at)
            .map(|timestamp| {
                (timestamp + chrono::Duration::milliseconds(DEFAULT_SWARM_LEASE_MS)).to_rfc3339()
            })
            .unwrap_or_else(|_| started_at.clone());
        if !app.db.claim_swarm_task(
            &candidate.task.id,
            &lease_owner,
            &lease_expires_at,
            &candidate.resolved_lane,
            &started_at,
        )? {
            continue;
        }

        if let Some(mut run) = app.db.get_swarm_run(&candidate.run.id)? {
            if run.status == SwarmRunStatus::Pending {
                run.status = SwarmRunStatus::Active;
                run.updated_at = started_at.clone();
                app.db.update_swarm_run(&run)?;
            }
        }

        summary.tasks_claimed += 1;
        let result = execute_swarm_task(app, executor, candidate)?;
        if result.ok {
            summary.tasks_completed += 1;
        } else {
            summary.tasks_failed += 1;
        }
    }

    summary.runs_considered = runs_considered.len();
    Ok(summary)
}

fn build_swarm_plan(
    input: &CreateSwarmObjectiveRunInput,
    run_id: &str,
    now: &str,
) -> Result<(Vec<SwarmTask>, Vec<SwarmTaskDependency>)> {
    if !input.tasks.is_empty() {
        let mut tasks = Vec::with_capacity(input.tasks.len());
        let mut ids_by_key = HashMap::new();
        for (index, task) in input.tasks.iter().enumerate() {
            let key = slugify_task_key(task.key.as_deref().unwrap_or(&task.title));
            let metadata = merge_plan_metadata(task);
            let swarm_task = SwarmTask {
                id: format!("{run_id}:{key}"),
                run_id: run_id.to_string(),
                task_key: key.clone(),
                title: task.title.clone(),
                prompt: task.prompt.clone(),
                command: task.command.clone(),
                requested_lane: task.lane.clone().unwrap_or(SwarmRequestedLane::Auto),
                resolved_lane: None,
                status: SwarmTaskStatus::Pending,
                priority: task.priority.unwrap_or(50 - index as i64),
                target_group_folder: task
                    .target_group_folder
                    .clone()
                    .unwrap_or_else(|| input.group_folder.clone()),
                target_chat_jid: input.chat_jid.clone(),
                cwd: task.cwd.clone(),
                repo: task.repo.clone(),
                repo_path: task.repo_path.clone(),
                sync: task.sync.unwrap_or(false),
                host: task.host.clone(),
                user: task.user.clone(),
                port: task.port,
                timeout_ms: task.timeout_ms,
                metadata,
                result: None,
                error: None,
                lease_owner: None,
                lease_expires_at: None,
                attempts: 0,
                max_attempts: task.max_attempts.unwrap_or(DEFAULT_SWARM_MAX_ATTEMPTS),
                created_at: now.to_string(),
                updated_at: now.to_string(),
                started_at: None,
                completed_at: None,
            };
            ids_by_key.insert(key, swarm_task.id.clone());
            tasks.push(swarm_task);
        }

        let mut dependencies = Vec::new();
        for (index, task) in input.tasks.iter().enumerate() {
            let task_id = tasks[index].id.clone();
            for dependency in task.depends_on.clone().unwrap_or_default() {
                let normalized = slugify_task_key(&dependency);
                if let Some(depends_on_task_id) = ids_by_key.get(&normalized) {
                    dependencies.push(SwarmTaskDependency {
                        task_id: task_id.clone(),
                        depends_on_task_id: depends_on_task_id.clone(),
                    });
                }
            }
        }
        return Ok((tasks, dependencies));
    }

    Ok(build_default_swarm_plan(input, run_id, now))
}

fn build_default_swarm_plan(
    input: &CreateSwarmObjectiveRunInput,
    run_id: &str,
    now: &str,
) -> (Vec<SwarmTask>, Vec<SwarmTaskDependency>) {
    let request_planes = detect_request_plane_requirements(&input.objective);
    let analysis_task = SwarmTask {
        id: format!("{run_id}:analysis"),
        run_id: run_id.to_string(),
        task_key: "analysis".to_string(),
        title: "Analyze objective".to_string(),
        prompt: Some(format!(
            "Analyze this swarm objective, identify the highest-risk assumptions, and produce a concise execution brief.\n\nObjective:\n{}",
            input.objective
        )),
        command: None,
        requested_lane: SwarmRequestedLane::Agent,
        resolved_lane: None,
        status: SwarmTaskStatus::Pending,
        priority: 90,
        target_group_folder: input.group_folder.clone(),
        target_chat_jid: input.chat_jid.clone(),
        cwd: None,
        repo: None,
        repo_path: None,
        sync: false,
        host: None,
        user: None,
        port: None,
        timeout_ms: None,
        metadata: Some(json!({ "requestPlane": "none" })),
        result: None,
        error: None,
        lease_owner: None,
        lease_expires_at: None,
        attempts: 0,
        max_attempts: DEFAULT_SWARM_MAX_ATTEMPTS,
        created_at: now.to_string(),
        updated_at: now.to_string(),
        started_at: None,
        completed_at: None,
    };

    if request_planes.contains(&RequestPlane::Web) && request_planes.contains(&RequestPlane::Email)
    {
        let web_task = SwarmTask {
            id: format!("{run_id}:web-execution"),
            run_id: run_id.to_string(),
            task_key: "web-execution".to_string(),
            title: "Handle web request work".to_string(),
            prompt: Some(build_plane_scoped_prompt(
                "Handle web request work",
                &format!(
                    "Complete only the web-facing portion of this objective.\n\nObjective:\n{}",
                    input.objective
                ),
                &RequestPlane::Web,
            )),
            command: None,
            requested_lane: SwarmRequestedLane::Agent,
            resolved_lane: None,
            status: SwarmTaskStatus::Pending,
            priority: 75,
            target_group_folder: input.group_folder.clone(),
            target_chat_jid: input.chat_jid.clone(),
            cwd: None,
            repo: None,
            repo_path: None,
            sync: false,
            host: None,
            user: None,
            port: None,
            timeout_ms: None,
            metadata: Some(json!({ "requestPlane": "web", "autoSplit": true })),
            result: None,
            error: None,
            lease_owner: None,
            lease_expires_at: None,
            attempts: 0,
            max_attempts: DEFAULT_SWARM_MAX_ATTEMPTS,
            created_at: now.to_string(),
            updated_at: now.to_string(),
            started_at: None,
            completed_at: None,
        };
        let email_task = SwarmTask {
            id: format!("{run_id}:email-execution"),
            run_id: run_id.to_string(),
            task_key: "email-execution".to_string(),
            title: "Handle email request work".to_string(),
            prompt: Some(build_plane_scoped_prompt(
                "Handle email request work",
                &format!(
                    "Complete only the email-facing portion of this objective.\n\nObjective:\n{}",
                    input.objective
                ),
                &RequestPlane::Email,
            )),
            command: None,
            requested_lane: SwarmRequestedLane::Agent,
            resolved_lane: None,
            status: SwarmTaskStatus::Pending,
            priority: 75,
            target_group_folder: input.group_folder.clone(),
            target_chat_jid: input.chat_jid.clone(),
            cwd: None,
            repo: None,
            repo_path: None,
            sync: false,
            host: None,
            user: None,
            port: None,
            timeout_ms: None,
            metadata: Some(json!({ "requestPlane": "email", "autoSplit": true })),
            result: None,
            error: None,
            lease_owner: None,
            lease_expires_at: None,
            attempts: 0,
            max_attempts: DEFAULT_SWARM_MAX_ATTEMPTS,
            created_at: now.to_string(),
            updated_at: now.to_string(),
            started_at: None,
            completed_at: None,
        };
        let verification_task = SwarmTask {
            id: format!("{run_id}:verification"),
            run_id: run_id.to_string(),
            task_key: "verification".to_string(),
            title: "Merge and verify outcome".to_string(),
            prompt: Some(build_plane_scoped_prompt(
                "Merge and verify outcome",
                &format!(
                    "Merge the web and email outputs, verify completeness, and note residual risk.\n\nObjective:\n{}",
                    input.objective
                ),
                &RequestPlane::None,
            )),
            command: None,
            requested_lane: SwarmRequestedLane::Agent,
            resolved_lane: None,
            status: SwarmTaskStatus::Pending,
            priority: 50,
            target_group_folder: input.group_folder.clone(),
            target_chat_jid: input.chat_jid.clone(),
            cwd: None,
            repo: None,
            repo_path: None,
            sync: false,
            host: None,
            user: None,
            port: None,
            timeout_ms: None,
            metadata: Some(json!({ "requestPlane": "none" })),
            result: None,
            error: None,
            lease_owner: None,
            lease_expires_at: None,
            attempts: 0,
            max_attempts: DEFAULT_SWARM_MAX_ATTEMPTS,
            created_at: now.to_string(),
            updated_at: now.to_string(),
            started_at: None,
            completed_at: None,
        };
        return (
            vec![
                analysis_task.clone(),
                web_task.clone(),
                email_task.clone(),
                verification_task.clone(),
            ],
            vec![
                SwarmTaskDependency {
                    task_id: web_task.id.clone(),
                    depends_on_task_id: analysis_task.id.clone(),
                },
                SwarmTaskDependency {
                    task_id: email_task.id.clone(),
                    depends_on_task_id: analysis_task.id.clone(),
                },
                SwarmTaskDependency {
                    task_id: verification_task.id.clone(),
                    depends_on_task_id: web_task.id.clone(),
                },
                SwarmTaskDependency {
                    task_id: verification_task.id.clone(),
                    depends_on_task_id: email_task.id.clone(),
                },
            ],
        );
    }

    let execution_request_plane = if request_planes.len() == 1 {
        request_planes[0].clone()
    } else {
        RequestPlane::None
    };
    let execution_task = SwarmTask {
        id: format!("{run_id}:execution"),
        run_id: run_id.to_string(),
        task_key: "execution".to_string(),
        title: "Execute objective".to_string(),
        prompt: Some(input.objective.clone()),
        command: None,
        requested_lane: input
            .requested_lane
            .clone()
            .unwrap_or(SwarmRequestedLane::Auto),
        resolved_lane: None,
        status: SwarmTaskStatus::Pending,
        priority: 70,
        target_group_folder: input.group_folder.clone(),
        target_chat_jid: input.chat_jid.clone(),
        cwd: None,
        repo: None,
        repo_path: None,
        sync: false,
        host: None,
        user: None,
        port: None,
        timeout_ms: None,
        metadata: Some(json!({ "requestPlane": execution_request_plane.as_str() })),
        result: None,
        error: None,
        lease_owner: None,
        lease_expires_at: None,
        attempts: 0,
        max_attempts: DEFAULT_SWARM_MAX_ATTEMPTS,
        created_at: now.to_string(),
        updated_at: now.to_string(),
        started_at: None,
        completed_at: None,
    };
    let verification_task = SwarmTask {
        id: format!("{run_id}:verification"),
        run_id: run_id.to_string(),
        task_key: "verification".to_string(),
        title: "Verify outcome".to_string(),
        prompt: Some(format!(
            "Verify whether this swarm objective is complete and note any residual risk.\n\nObjective:\n{}",
            input.objective
        )),
        command: None,
        requested_lane: SwarmRequestedLane::Agent,
        resolved_lane: None,
        status: SwarmTaskStatus::Pending,
        priority: 50,
        target_group_folder: input.group_folder.clone(),
        target_chat_jid: input.chat_jid.clone(),
        cwd: None,
        repo: None,
        repo_path: None,
        sync: false,
        host: None,
        user: None,
        port: None,
        timeout_ms: None,
        metadata: Some(json!({ "requestPlane": "none" })),
        result: None,
        error: None,
        lease_owner: None,
        lease_expires_at: None,
        attempts: 0,
        max_attempts: DEFAULT_SWARM_MAX_ATTEMPTS,
        created_at: now.to_string(),
        updated_at: now.to_string(),
        started_at: None,
        completed_at: None,
    };
    (
        vec![
            analysis_task.clone(),
            execution_task.clone(),
            verification_task.clone(),
        ],
        vec![
            SwarmTaskDependency {
                task_id: execution_task.id.clone(),
                depends_on_task_id: analysis_task.id.clone(),
            },
            SwarmTaskDependency {
                task_id: verification_task.id.clone(),
                depends_on_task_id: execution_task.id.clone(),
            },
        ],
    )
}

fn build_runnable_tasks(
    app: &mut NanoclawApp,
    runs: &[SwarmRun],
) -> Result<Vec<RunnableSwarmTask>> {
    let mut runnable = Vec::new();

    for run in runs {
        let mut tasks = app.db.list_swarm_tasks(&run.id)?;
        let dependencies = app.db.list_swarm_task_dependencies(&run.id)?;
        let dependency_map = build_dependency_map(&dependencies);
        block_tasks_with_failed_dependencies(app, &tasks, &dependency_map)?;
        tasks = app.db.list_swarm_tasks(&run.id)?;
        reconcile_swarm_run_completion(app, run, &tasks)?;

        let task_map = tasks
            .iter()
            .cloned()
            .map(|task| (task.id.clone(), task))
            .collect::<HashMap<_, _>>();
        for task in tasks {
            if !matches!(
                task.status,
                SwarmTaskStatus::Pending | SwarmTaskStatus::Ready
            ) {
                continue;
            }
            let Some(dependency_results) =
                all_task_dependencies_completed(&task, &task_map, &dependency_map)
            else {
                continue;
            };
            if matches!(task.status, SwarmTaskStatus::Pending) {
                let mut ready = task.clone();
                ready.status = SwarmTaskStatus::Ready;
                ready.updated_at = Utc::now().to_rfc3339();
                app.db.update_swarm_task(&ready)?;
            }
            runnable.push(RunnableSwarmTask {
                run: run.clone(),
                resolved_lane: resolve_swarm_task_lane(&task, Some(&run.objective)),
                task,
                dependencies: dependency_results,
            });
        }
    }

    runnable.sort_by(|left, right| {
        right
            .task
            .priority
            .cmp(&left.task.priority)
            .then_with(|| left.task.created_at.cmp(&right.task.created_at))
    });
    Ok(runnable)
}

fn execute_swarm_task<E: ExecutorBoundary>(
    app: &mut NanoclawApp,
    executor: &E,
    runnable: RunnableSwarmTask,
) -> Result<SwarmTaskExecutionResult> {
    let result = match match runnable.resolved_lane {
        SwarmResolvedLane::Agent => run_swarm_agent_lane(app, executor, &runnable),
        SwarmResolvedLane::Codex => run_swarm_codex_lane(app, executor, &runnable),
        SwarmResolvedLane::Host => run_swarm_host_lane(app, executor, &runnable),
        SwarmResolvedLane::RepoMirror => run_swarm_repo_mirror_lane(app, &runnable),
        SwarmResolvedLane::Symphony => run_swarm_symphony_lane(app, &runnable),
        SwarmResolvedLane::Custom(ref value) => Ok(SwarmTaskExecutionResult {
            ok: false,
            summary: format!("Unsupported swarm lane: {value}"),
            output: None,
            error: Some(format!("Unsupported swarm lane: {value}")),
            metadata: None,
            non_retryable: true,
        }),
    } {
        Ok(result) => result,
        Err(error) => SwarmTaskExecutionResult {
            ok: false,
            summary: error.to_string(),
            output: None,
            error: Some(error.to_string()),
            metadata: None,
            non_retryable: false,
        },
    };

    let Some(mut stored_task) = app.db.get_swarm_task(&runnable.task.id)? else {
        bail!(
            "swarm task {} disappeared during execution",
            runnable.task.id
        );
    };
    let completed_at = Utc::now().to_rfc3339();
    stored_task.resolved_lane = Some(runnable.resolved_lane.clone());
    stored_task.result = Some(json!({
        "summary": result.summary,
        "output": result.output,
        "metadata": result.metadata,
    }));
    stored_task.error = result.error.clone();
    stored_task.lease_owner = None;
    stored_task.lease_expires_at = None;
    stored_task.updated_at = completed_at.clone();

    if result.ok {
        stored_task.status = SwarmTaskStatus::Completed;
        stored_task.completed_at = Some(completed_at);
    } else if result.non_retryable || stored_task.attempts >= stored_task.max_attempts {
        stored_task.status = SwarmTaskStatus::Failed;
        stored_task.completed_at = Some(completed_at);
    } else {
        stored_task.status = SwarmTaskStatus::Ready;
        stored_task.completed_at = None;
    }
    app.db.update_swarm_task(&stored_task)?;
    if let Some(run) = app.db.get_swarm_run(&runnable.run.id)? {
        let tasks = app.db.list_swarm_tasks(&run.id)?;
        reconcile_swarm_run_completion(app, &run, &tasks)?;
    }
    Ok(result)
}

fn run_swarm_agent_lane<E: ExecutorBoundary>(
    app: &mut NanoclawApp,
    executor: &E,
    runnable: &RunnableSwarmTask,
) -> Result<SwarmTaskExecutionResult> {
    let base_group = ensure_group_by_folder(app, &runnable.task.target_group_folder)?;
    let group = prepare_swarm_agent_group(app, &base_group, &runnable.task)?;
    let workspace_root = app.config.groups_dir.join(&group.folder);
    let request_plane = task_request_plane(&runnable.task);
    let prompt = build_agent_task_prompt(&runnable.run, &runnable.task, &runnable.dependencies);
    if let Some(error) = get_request_plane_text_error(&prompt, &request_plane) {
        return Ok(SwarmTaskExecutionResult {
            ok: false,
            summary: error.clone(),
            output: None,
            error: Some(error),
            metadata: None,
            non_retryable: true,
        });
    }
    let response = execute_swarm_prompt(
        app,
        executor,
        &group,
        &workspace_root,
        &prompt,
        request_plane,
        Some(select_agent_backend()),
    )?;
    Ok(execution_response_to_task_result(response))
}

fn run_swarm_codex_lane<E: ExecutorBoundary>(
    app: &mut NanoclawApp,
    executor: &E,
    runnable: &RunnableSwarmTask,
) -> Result<SwarmTaskExecutionResult> {
    let group = ensure_group_by_folder(app, &runnable.task.target_group_folder)?;
    let workspace_root =
        resolve_task_workspace_root(app, &group.folder, runnable.task.cwd.as_deref());
    let request_plane = task_request_plane(&runnable.task);
    let prompt = build_codex_task_prompt(&runnable.run, &runnable.task, &runnable.dependencies);
    if let Some(error) = get_request_plane_text_error(&prompt, &request_plane) {
        return Ok(SwarmTaskExecutionResult {
            ok: false,
            summary: error.clone(),
            output: None,
            error: Some(error),
            metadata: None,
            non_retryable: true,
        });
    }
    let response = execute_swarm_prompt(
        app,
        executor,
        &group,
        &workspace_root,
        &prompt,
        request_plane,
        Some(select_codex_backend()),
    )?;
    Ok(execution_response_to_task_result(response))
}

fn run_swarm_host_lane<E: ExecutorBoundary>(
    app: &mut NanoclawApp,
    executor: &E,
    runnable: &RunnableSwarmTask,
) -> Result<SwarmTaskExecutionResult> {
    let Some(command) = runnable.task.command.as_deref().map(str::trim) else {
        return Ok(SwarmTaskExecutionResult {
            ok: false,
            summary: "Host lane requires a command.".to_string(),
            output: None,
            error: Some("Host lane requires a command.".to_string()),
            metadata: None,
            non_retryable: true,
        });
    };
    let request_plane = task_request_plane(&runnable.task);
    if let Some(violation) = command_violates_request_plane(command, &request_plane) {
        let error = match violation {
            RequestPlane::Web => {
                "This worker is request-isolated and cannot take on web-request command work."
            }
            RequestPlane::Email => {
                "This worker is request-isolated and cannot take on email-request command work."
            }
            _ => "This worker cannot take on cross-plane command work.",
        };
        return Ok(SwarmTaskExecutionResult {
            ok: false,
            summary: error.to_string(),
            output: None,
            error: Some(error.to_string()),
            metadata: None,
            non_retryable: true,
        });
    }
    let group = ensure_group_by_folder(app, &runnable.task.target_group_folder)?;
    let workspace_root =
        resolve_task_workspace_root(app, &group.folder, runnable.task.cwd.as_deref());
    let response = execute_swarm_script(
        app,
        executor,
        &group,
        &workspace_root,
        command,
        request_plane,
    )?;
    Ok(execution_response_to_task_result(response))
}

fn run_swarm_repo_mirror_lane(
    app: &mut NanoclawApp,
    runnable: &RunnableSwarmTask,
) -> Result<SwarmTaskExecutionResult> {
    let Some(command) = runnable.task.command.as_deref().map(str::trim) else {
        return Ok(SwarmTaskExecutionResult {
            ok: false,
            summary: "Repo mirror lane requires a command.".to_string(),
            output: None,
            error: Some("Repo mirror lane requires a command.".to_string()),
            metadata: None,
            non_retryable: true,
        });
    };
    let request_plane = task_request_plane(&runnable.task);
    if request_plane != RequestPlane::Web {
        let error = "Repo mirror tasks currently require a web-scoped worker because they may fetch, pull, clone, or sync over the network.";
        return Ok(SwarmTaskExecutionResult {
            ok: false,
            summary: error.to_string(),
            output: None,
            error: Some(error.to_string()),
            metadata: None,
            non_retryable: true,
        });
    }
    let environment = DigitalOceanDevEnvironment::from_config(&app.config);
    let remote_root = if runnable.task.sync {
        let _ = environment.sync_project()?;
        environment.remote_repo_path()?
    } else if let Some(repo) = runnable
        .task
        .repo
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        let remote_root = build_remote_repo_mirror_path(&app.config.droplet_repo_root, repo)?;
        ensure_remote_repo_mirror(&environment, repo, &remote_root)?;
        remote_root
    } else {
        environment.remote_repo_path()?
    };
    let remote_cwd = resolve_remote_cwd(&remote_root, runnable.task.repo_path.as_deref());
    let result = environment.exec(&format!("cd {} && {}", shell_quote(&remote_cwd), command));
    match result {
        Ok(exec) => Ok(SwarmTaskExecutionResult {
            ok: true,
            summary: summarize_text(&exec.stdout),
            output: trim_output(&exec.stdout),
            error: None,
            metadata: Some(json!({
                "remote_target": exec.remote_target,
                "remote_repo_path": remote_cwd,
                "mode": "repo_mirror",
            })),
            non_retryable: false,
        }),
        Err(error) => Ok(SwarmTaskExecutionResult {
            ok: false,
            summary: error.to_string(),
            output: None,
            error: Some(error.to_string()),
            metadata: Some(json!({
                "remote_repo_path": remote_cwd,
                "mode": "repo_mirror",
            })),
            non_retryable: false,
        }),
    }
}

fn run_swarm_symphony_lane(
    app: &mut NanoclawApp,
    runnable: &RunnableSwarmTask,
) -> Result<SwarmTaskExecutionResult> {
    let Some(command) = runnable.task.command.as_deref().map(str::trim) else {
        return Ok(SwarmTaskExecutionResult {
            ok: false,
            summary: "Symphony lane requires a command.".to_string(),
            output: None,
            error: Some("Symphony lane requires a command.".to_string()),
            metadata: None,
            non_retryable: true,
        });
    };
    let environment = DigitalOceanDevEnvironment::from_config(&app.config);
    let remote_root = environment.remote_repo_path()?;
    let remote_cwd = resolve_remote_cwd(&remote_root, runnable.task.cwd.as_deref());
    let result = environment.exec(&format!("cd {} && {}", shell_quote(&remote_cwd), command));
    match result {
        Ok(exec) => Ok(SwarmTaskExecutionResult {
            ok: true,
            summary: summarize_text(&exec.stdout),
            output: trim_output(&exec.stdout),
            error: None,
            metadata: Some(json!({
                "remote_target": exec.remote_target,
                "remote_repo_path": remote_cwd,
                "mode": "symphony",
            })),
            non_retryable: false,
        }),
        Err(error) => Ok(SwarmTaskExecutionResult {
            ok: false,
            summary: error.to_string(),
            output: None,
            error: Some(error.to_string()),
            metadata: Some(json!({
                "remote_repo_path": remote_cwd,
                "mode": "symphony",
            })),
            non_retryable: false,
        }),
    }
}

fn execute_swarm_prompt<E: ExecutorBoundary>(
    app: &NanoclawApp,
    executor: &E,
    group: &Group,
    workspace_root: &Path,
    prompt: &str,
    request_plane: RequestPlane,
    backend_override: Option<WorkerBackend>,
) -> Result<ExecutionResponse> {
    let session = build_execution_session(
        &app.config.data_dir,
        &group.folder,
        &format!("swarm-session-{}", Uuid::new_v4()),
        workspace_root,
    );
    session.ensure_layout()?;
    executor.execute(ExecutionRequest {
        group: group.clone(),
        prompt: prompt.to_string(),
        messages: Vec::new(),
        script: None,
        omx: None,
        assistant_name: app.config.assistant_name.clone(),
        request_plane,
        session,
        backend_override,
    })
}

fn execute_swarm_script<E: ExecutorBoundary>(
    app: &NanoclawApp,
    executor: &E,
    group: &Group,
    workspace_root: &Path,
    script: &str,
    request_plane: RequestPlane,
) -> Result<ExecutionResponse> {
    let session = build_execution_session(
        &app.config.data_dir,
        &group.folder,
        &format!("swarm-session-{}", Uuid::new_v4()),
        workspace_root,
    );
    session.ensure_layout()?;
    executor.execute(ExecutionRequest {
        group: group.clone(),
        prompt: format!("Run swarm command task for {}", group.name),
        messages: Vec::new(),
        script: Some(script.to_string()),
        omx: None,
        assistant_name: app.config.assistant_name.clone(),
        request_plane,
        session,
        backend_override: None,
    })
}

fn execution_response_to_task_result(response: ExecutionResponse) -> SwarmTaskExecutionResult {
    SwarmTaskExecutionResult {
        ok: true,
        summary: summarize_text(&response.text),
        output: trim_output(&response.text),
        error: None,
        metadata: Some(json!({
            "session_id": response.session_id,
            "boundary": format!("{:?}", response.boundary.kind),
            "log_path": response.log_path,
            "provenance_id": response.provenance.as_ref().map(|value| value.id.clone()),
        })),
        non_retryable: false,
    }
}

fn block_tasks_with_failed_dependencies(
    app: &mut NanoclawApp,
    tasks: &[SwarmTask],
    dependency_map: &HashMap<String, Vec<String>>,
) -> Result<()> {
    let task_map = tasks
        .iter()
        .cloned()
        .map(|task| (task.id.clone(), task))
        .collect::<HashMap<_, _>>();
    for task in tasks {
        if !matches!(
            task.status,
            SwarmTaskStatus::Pending | SwarmTaskStatus::Ready
        ) {
            continue;
        }
        let dependency_ids = dependency_map.get(&task.id).cloned().unwrap_or_default();
        if dependency_ids.is_empty() {
            continue;
        }
        let failed_dependencies = dependency_ids
            .into_iter()
            .filter_map(|dependency_id| task_map.get(&dependency_id))
            .filter(|dependency| {
                matches!(
                    dependency.status,
                    SwarmTaskStatus::Failed | SwarmTaskStatus::Blocked | SwarmTaskStatus::Canceled
                )
            })
            .map(|dependency| dependency.task_key.clone())
            .collect::<Vec<_>>();
        if failed_dependencies.is_empty() {
            continue;
        }

        let mut blocked = task.clone();
        blocked.status = SwarmTaskStatus::Blocked;
        blocked.error = Some(format!(
            "Blocked by failed dependencies: {}",
            failed_dependencies.join(", ")
        ));
        blocked.updated_at = Utc::now().to_rfc3339();
        app.db.update_swarm_task(&blocked)?;
    }
    Ok(())
}

fn all_task_dependencies_completed(
    task: &SwarmTask,
    task_map: &HashMap<String, SwarmTask>,
    dependency_map: &HashMap<String, Vec<String>>,
) -> Option<Vec<SwarmTask>> {
    let dependencies = dependency_map
        .get(&task.id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|dependency_id| task_map.get(&dependency_id).cloned())
        .collect::<Vec<_>>();
    for dependency in &dependencies {
        if matches!(
            dependency.status,
            SwarmTaskStatus::Failed | SwarmTaskStatus::Blocked | SwarmTaskStatus::Canceled
        ) {
            return None;
        }
        if !matches!(dependency.status, SwarmTaskStatus::Completed) {
            return None;
        }
    }
    Some(dependencies)
}

fn reconcile_swarm_run_completion(
    app: &NanoclawApp,
    run: &SwarmRun,
    tasks: &[SwarmTask],
) -> Result<()> {
    if tasks.is_empty() {
        return Ok(());
    }
    if !tasks.iter().all(|task| {
        matches!(
            task.status,
            SwarmTaskStatus::Completed
                | SwarmTaskStatus::Failed
                | SwarmTaskStatus::Blocked
                | SwarmTaskStatus::Canceled
        )
    }) {
        return Ok(());
    }
    if matches!(
        run.status,
        SwarmRunStatus::Completed | SwarmRunStatus::Failed | SwarmRunStatus::Canceled
    ) {
        return Ok(());
    }

    let completed = tasks
        .iter()
        .filter(|task| matches!(task.status, SwarmTaskStatus::Completed))
        .count();
    let failed = tasks
        .iter()
        .filter(|task| matches!(task.status, SwarmTaskStatus::Failed))
        .count();
    let blocked = tasks
        .iter()
        .filter(|task| matches!(task.status, SwarmTaskStatus::Blocked))
        .count();
    let canceled = tasks
        .iter()
        .filter(|task| matches!(task.status, SwarmTaskStatus::Canceled))
        .count();
    let status = if failed > 0 || blocked > 0 {
        SwarmRunStatus::Failed
    } else if canceled == tasks.len() {
        SwarmRunStatus::Canceled
    } else {
        SwarmRunStatus::Completed
    };
    let summary = if status == SwarmRunStatus::Completed {
        format!(
            "Swarm run completed: {}/{} tasks succeeded.",
            completed,
            tasks.len()
        )
    } else if status == SwarmRunStatus::Canceled {
        format!("Swarm run canceled after {} task(s).", canceled)
    } else {
        format!(
            "Swarm run failed: {} failed, {} blocked, {} completed.",
            failed, blocked, completed
        )
    };
    let mut updated = run.clone();
    updated.status = status.clone();
    updated.summary = Some(summary);
    updated.result = Some(json!({
        "objective": run.objective,
        "status": status.as_str(),
        "counts": {
            "total": tasks.len(),
            "completed": completed,
            "failed": failed,
            "blocked": blocked,
            "canceled": canceled,
        },
        "tasks": tasks.iter().map(|task| {
            json!({
                "key": task.task_key,
                "title": task.title,
                "status": task.status.as_str(),
                "lane": task
                    .resolved_lane
                    .as_ref()
                    .map(SwarmResolvedLane::as_str)
                    .unwrap_or_else(|| task.requested_lane.as_str()),
                "summary": extract_task_summary(task),
                "error": task.error,
            })
        }).collect::<Vec<_>>(),
    }));
    updated.updated_at = Utc::now().to_rfc3339();
    updated.completed_at = Some(updated.updated_at.clone());
    app.db.update_swarm_run(&updated)?;
    Ok(())
}

fn resolve_swarm_task_lane(task: &SwarmTask, run_objective: Option<&str>) -> SwarmResolvedLane {
    if !matches!(task.requested_lane, SwarmRequestedLane::Auto) {
        return match &task.requested_lane {
            SwarmRequestedLane::Agent => SwarmResolvedLane::Agent,
            SwarmRequestedLane::Codex => SwarmResolvedLane::Codex,
            SwarmRequestedLane::Host => SwarmResolvedLane::Host,
            SwarmRequestedLane::RepoMirror => SwarmResolvedLane::RepoMirror,
            SwarmRequestedLane::Symphony => SwarmResolvedLane::Symphony,
            SwarmRequestedLane::Custom(value) => SwarmResolvedLane::Custom(value.clone()),
            SwarmRequestedLane::Auto => SwarmResolvedLane::Agent,
        };
    }

    let haystack = [
        run_objective.unwrap_or(""),
        &task.title,
        task.prompt.as_deref().unwrap_or(""),
        task.command.as_deref().unwrap_or(""),
        task.cwd.as_deref().unwrap_or(""),
        task.repo.as_deref().unwrap_or(""),
        task.repo_path.as_deref().unwrap_or(""),
        task.host.as_deref().unwrap_or(""),
    ]
    .join(" ")
    .to_ascii_lowercase();

    if task
        .command
        .as_deref()
        .map(str::trim)
        .unwrap_or("")
        .is_empty()
        .not()
    {
        if task.host.is_some()
            || haystack.contains("droplet")
            || haystack.contains("symphony")
            || haystack.contains("systemctl")
            || haystack.contains("journalctl")
        {
            return SwarmResolvedLane::Symphony;
        }
        if task.repo.is_some() || task.repo_path.is_some() || task.sync {
            return SwarmResolvedLane::RepoMirror;
        }
        return SwarmResolvedLane::Host;
    }

    if task.repo.is_some() || task.repo_path.is_some() || task.cwd.is_some() {
        return SwarmResolvedLane::Codex;
    }

    if [
        "fix",
        "refactor",
        "implement",
        "repo",
        "code",
        "pull request",
        "build",
    ]
    .iter()
    .any(|needle| haystack.contains(needle))
    {
        return SwarmResolvedLane::Codex;
    }

    SwarmResolvedLane::Agent
}

fn task_request_plane(task: &SwarmTask) -> RequestPlane {
    if let Some(value) = task
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("requestPlane"))
        .and_then(Value::as_str)
        .and_then(normalize_request_plane)
    {
        return value;
    }

    let inferred = detect_request_plane_requirements(
        &[
            &task.title,
            task.prompt.as_deref().unwrap_or(""),
            task.command.as_deref().unwrap_or(""),
        ]
        .join("\n\n"),
    );
    if inferred.len() == 1 {
        inferred[0].clone()
    } else {
        RequestPlane::None
    }
}

fn build_agent_task_prompt(run: &SwarmRun, task: &SwarmTask, dependencies: &[SwarmTask]) -> String {
    let mut segments = vec![
        format!("Swarm objective:\n{}", run.objective),
        task.prompt
            .clone()
            .map(|value| format!("Assigned task:\n{}", value))
            .unwrap_or_else(|| format!("Assigned task title: {}", task.title)),
    ];
    let dependency_context = build_dependency_result_context(dependencies);
    if !dependency_context.is_empty() {
        segments.push(format!(
            "Completed dependency results:\n{}",
            dependency_context
        ));
    }
    segments.push(
        "Return a concise execution summary with concrete outcomes, blockers, and next steps. Stay inside the assigned task scope instead of re-planning the whole swarm.".to_string(),
    );
    segments.join("\n\n")
}

fn build_codex_task_prompt(run: &SwarmRun, task: &SwarmTask, dependencies: &[SwarmTask]) -> String {
    let mut segments = vec![
        format!("Swarm objective:\n{}", run.objective),
        task.prompt
            .clone()
            .map(|value| format!("Assigned code task:\n{}", value))
            .unwrap_or_else(|| format!("Assigned code task title: {}", task.title)),
    ];
    let dependency_context = build_dependency_result_context(dependencies);
    if !dependency_context.is_empty() {
        segments.push(format!("Dependency outputs:\n{}", dependency_context));
    }
    segments.push(
        "Apply the minimum coherent set of changes for this task. At the end, report what changed, what was verified, and any remaining risk.".to_string(),
    );
    segments.join("\n\n")
}

fn build_dependency_result_context(dependencies: &[SwarmTask]) -> String {
    dependencies
        .iter()
        .map(|dependency| {
            format!(
                "- {} [{}] ({}): {}",
                dependency.title,
                dependency.task_key,
                dependency
                    .resolved_lane
                    .as_ref()
                    .map(SwarmResolvedLane::as_str)
                    .unwrap_or_else(|| dependency.requested_lane.as_str()),
                extract_task_summary(dependency)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn build_plane_scoped_prompt(title: &str, body: &str, request_plane: &RequestPlane) -> String {
    let scope_instruction = match request_plane {
        RequestPlane::Web => {
            "Handle only the web-request portion of this work. Do not perform or plan any email actions."
        }
        RequestPlane::Email => {
            "Handle only the email-request portion of this work. Do not perform or plan any web fetch, browser, or web lookup actions."
        }
        _ => {
            "Handle only the coordination and merge portion of this work. Do not perform direct web or email request actions."
        }
    };
    format!("Task: {title}\n\n{}\n\n{}", body.trim(), scope_instruction)
}

fn prepare_swarm_agent_group(
    app: &mut NanoclawApp,
    base_group: &Group,
    task: &SwarmTask,
) -> Result<Group> {
    let suffix = uuid_sha_suffix(&task.id);
    let folder = format!("swarm_agent_{}", suffix);
    let group_dir = app.config.groups_dir.join(&folder);
    fs::create_dir_all(&group_dir)
        .with_context(|| format!("failed to create {}", group_dir.display()))?;

    let source_claude = app
        .config
        .groups_dir
        .join(&base_group.folder)
        .join("CLAUDE.md");
    let target_claude = group_dir.join("CLAUDE.md");
    if source_claude.exists() && !target_claude.exists() {
        fs::copy(&source_claude, &target_claude).with_context(|| {
            format!(
                "failed to copy {} to {}",
                source_claude.display(),
                target_claude.display()
            )
        })?;
    }

    Ok(Group {
        jid: format!("swarm:{}:{}", task.run_id, task.task_key),
        name: format!("{} • Swarm {}", base_group.name, task.task_key),
        folder,
        trigger: base_group.trigger.clone(),
        added_at: Utc::now().to_rfc3339(),
        requires_trigger: false,
        is_main: false,
    })
}

fn ensure_group_by_folder(app: &mut NanoclawApp, folder: &str) -> Result<Group> {
    if let Some(group) = app
        .groups()?
        .into_iter()
        .find(|group| group.folder == folder)
    {
        return Ok(group);
    }

    app.register_group(Group {
        jid: folder.to_string(),
        name: folder.to_string(),
        folder: folder.to_string(),
        trigger: app.config.default_trigger.clone(),
        added_at: Utc::now().to_rfc3339(),
        requires_trigger: false,
        is_main: folder == "main",
    })
}

fn resolve_task_workspace_root(
    app: &NanoclawApp,
    group_folder: &str,
    cwd: Option<&str>,
) -> PathBuf {
    if let Some(cwd) = cwd.map(str::trim).filter(|value| !value.is_empty()) {
        let path = PathBuf::from(cwd);
        if path.is_absolute() {
            return path;
        }
        return app.config.project_root.join(path);
    }
    app.config.groups_dir.join(group_folder)
}

fn build_dependency_map(dependencies: &[SwarmTaskDependency]) -> HashMap<String, Vec<String>> {
    let mut map = HashMap::new();
    for dependency in dependencies {
        map.entry(dependency.task_id.clone())
            .or_insert_with(Vec::new)
            .push(dependency.depends_on_task_id.clone());
    }
    map
}

fn merge_plan_metadata(task: &SwarmPlanTaskInput) -> Option<Value> {
    let mut metadata = task.metadata.clone().unwrap_or_else(|| json!({}));
    if let Some(request_plane) = task.request_plane.as_ref() {
        if let Some(object) = metadata.as_object_mut() {
            object.insert(
                "requestPlane".to_string(),
                Value::String(request_plane.as_str().to_string()),
            );
        }
    }
    if metadata
        .as_object()
        .map(|value| value.is_empty())
        .unwrap_or(true)
    {
        None
    } else {
        Some(metadata)
    }
}

fn swarm_max_steps() -> usize {
    std::env::var("NANOCLAW_SWARM_MAX_STEPS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SWARM_MAX_STEPS)
}

fn select_agent_backend() -> WorkerBackend {
    std::env::var("NANOCLAW_SWARM_AGENT_BACKEND")
        .ok()
        .map(|value| WorkerBackend::parse(&value))
        .unwrap_or_else(|| {
            if is_command_available("codex") {
                WorkerBackend::Codex
            } else if is_command_available("claude") {
                WorkerBackend::Claude
            } else {
                WorkerBackend::Summary
            }
        })
}

fn select_codex_backend() -> WorkerBackend {
    std::env::var("NANOCLAW_SWARM_CODEX_BACKEND")
        .ok()
        .map(|value| WorkerBackend::parse(&value))
        .unwrap_or_else(|| {
            if is_command_available("codex") {
                WorkerBackend::Codex
            } else if is_command_available("claude") {
                WorkerBackend::Claude
            } else {
                WorkerBackend::Summary
            }
        })
}

fn slugify_task_key(value: &str) -> String {
    let mut slug = String::new();
    let mut last_dash = false;
    for ch in value.chars().flat_map(|ch| ch.to_lowercase()) {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
            last_dash = false;
        } else if !last_dash {
            slug.push('-');
            last_dash = true;
        }
    }
    slug.trim_matches('-')
        .chars()
        .take(48)
        .collect::<String>()
        .if_empty("task")
}

fn trim_output(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.chars().take(RESULT_OUTPUT_LIMIT).collect())
}

fn summarize_text(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "Completed with no textual output.".to_string();
    }
    let summary = trimmed
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or(trimmed);
    summary.chars().take(RESULT_SUMMARY_LIMIT).collect()
}

fn extract_task_summary(task: &SwarmTask) -> String {
    if let Some(summary) = task
        .result
        .as_ref()
        .and_then(|value| value.get("summary"))
        .and_then(Value::as_str)
    {
        return summary.to_string();
    }
    if let Some(error) = task.error.as_ref() {
        return format!("Error: {}", error);
    }
    "No result recorded.".to_string()
}

fn resolve_remote_cwd(root: &str, path: Option<&str>) -> String {
    let Some(path) = path.map(str::trim).filter(|value| !value.is_empty()) else {
        return root.to_string();
    };
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("{}/{}", root.trim_end_matches('/'), path)
    }
}

fn build_remote_repo_mirror_path(root: &str, repo: &str) -> Result<String> {
    let mut parts = repo.split('/');
    let Some(owner) = parts.next().filter(|value| !value.trim().is_empty()) else {
        bail!("invalid GitHub repository identifier '{}'", repo);
    };
    let Some(name) = parts.next().filter(|value| !value.trim().is_empty()) else {
        bail!("invalid GitHub repository identifier '{}'", repo);
    };
    Ok(format!(
        "{}/{}/{}",
        root.trim_end_matches('/'),
        owner.trim(),
        name.trim()
    ))
}

fn ensure_remote_repo_mirror(
    environment: &DigitalOceanDevEnvironment,
    repo: &str,
    remote_root: &str,
) -> Result<()> {
    environment.exec(&format!(
        "set -euo pipefail\nmkdir -p {parent}\nif [ -d {root}/.git ]; then\n  git config --global --add safe.directory {root} || true\n  git -C {root} fetch --all --prune\n  git -C {root} pull --ff-only\nelse\n  git clone git@github.com:{repo}.git {root} || git clone https://github.com/{repo}.git {root}\n  git config --global --add safe.directory {root} || true\nfi",
        parent = shell_quote(
            Path::new(remote_root)
                .parent()
                .unwrap_or_else(|| Path::new(remote_root))
                .display()
                .to_string()
                .as_str()
        ),
        root = shell_quote(remote_root),
        repo = repo.trim(),
    ))?;
    Ok(())
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn uuid_sha_suffix(value: &str) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(value.as_bytes());
    hash.iter()
        .take(6)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

trait StringExt {
    fn if_empty(self, fallback: &str) -> String;
}

impl StringExt for String {
    fn if_empty(self, fallback: &str) -> String {
        if self.trim().is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
}

trait BoolNot {
    fn not(self) -> bool;
}

impl BoolNot for bool {
    fn not(self) -> bool {
        !self
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use std::path::Path;
    use tempfile::tempdir;

    use super::{
        build_default_swarm_plan, create_swarm_objective_run, get_swarm_run_details,
        pump_swarm_once, CreateSwarmObjectiveRunInput,
    };
    use crate::foundation::{DevelopmentEnvironment, DevelopmentEnvironmentKind, RemoteWorkerMode};
    use crate::nanoclaw::app::NanoclawApp;
    use crate::nanoclaw::config::NanoclawConfig;
    use crate::nanoclaw::executor::InProcessEchoExecutor;

    fn test_config(project_root: &Path) -> NanoclawConfig {
        NanoclawConfig {
            project_root: project_root.to_path_buf(),
            data_dir: project_root.join("data"),
            groups_dir: project_root.join("groups"),
            store_dir: project_root.join("store"),
            db_path: project_root.join("store/messages.db"),
            assistant_name: "Andy".to_string(),
            default_trigger: "@Andy".to_string(),
            timezone: "UTC".to_string(),
            max_concurrent_groups: 4,
            execution_lane: crate::foundation::ExecutionLane::Host,
            container_image: "rust:1.75-slim".to_string(),
            container_runtime: None,
            container_groups: Vec::new(),
            droplet_ssh_host: "127.0.0.1".to_string(),
            droplet_ssh_user: "root".to_string(),
            droplet_ssh_port: 22,
            droplet_repo_root: "/srv/code-mirror".to_string(),
            remote_worker_mode: RemoteWorkerMode::Off,
            remote_worker_root: "/root/.nanoclaw-worker".to_string(),
            remote_worker_binary: "/usr/local/bin/nanoclaw-rs".to_string(),
            remote_worker_bootstrap_timeout_ms: 30_000,
            remote_worker_sync_interval_ms: 500,
            remote_worker_tunnel_port_base: 13_000,
            omx_runner_path: "/usr/local/bin/omx-paperclip-runner".to_string(),
            omx_state_root: "/root/.nanoclaw-omx".to_string(),
            omx_callback_url: "http://127.0.0.1:8788/webhook/omx".to_string(),
            omx_callback_token: String::new(),
            omx_default_mode: "team".to_string(),
            omx_default_max_workers: 3,
            omx_poll_interval_ms: 5_000,
            openclaw_gateway_bind_host: "127.0.0.1".to_string(),
            openclaw_gateway_public_host: "127.0.0.1".to_string(),
            openclaw_gateway_port: 0,
            openclaw_gateway_token: String::new(),
            openclaw_gateway_execution_lane: crate::foundation::ExecutionLane::Host,
            slack_env_file: None,
            slack_poll_interval_ms: 500,
            linear_webhook_port: 0,
            linear_webhook_secret: String::new(),
            github_webhook_secret: String::new(),
            linear_chat_jid: String::new(),
            linear_api_key: String::new(),
            linear_write_api_key: String::new(),
            linear_pm_team_keys: Vec::new(),
            linear_pm_policy_interval_minutes: 60,
            linear_pm_digest_interval_hours: 6,
            linear_pm_guardrail_max_automations: 10,
            observability_chat_jid: String::new(),
            observability_group_folder: "observability".to_string(),
            observability_webhook_token: String::new(),
            observability_auto_blue_team: true,
            observability_adapters_path: project_root.join("observability-adapters.json"),
            sender_allowlist_path: project_root.join("sender-allowlist.json"),
            project_environments_path: project_root.join("project-environments.json"),
            host_os_control_policy_path: project_root.join("host-os-policy.json"),
            host_os_approval_chat_jid: None,
            remote_control_ssh_host: None,
            remote_control_ssh_user: None,
            remote_control_ssh_port: 22,
            remote_control_workspace_root: None,
            development_environment: DevelopmentEnvironment {
                id: "local".to_string(),
                name: "Local".to_string(),
                kind: DevelopmentEnvironmentKind::Local,
                workspace_root: Some(project_root.display().to_string()),
                repo_root: Some("/srv/code-mirror/local/nanoclaw".to_string()),
                ssh: None,
                remote_worker_mode: RemoteWorkerMode::Off,
                remote_worker_root: Some("/root/.nanoclaw-worker".to_string()),
                bootstrap_timeout_ms: None,
                sync_interval_ms: None,
                tunnel_port_base: None,
            },
        }
    }

    #[test]
    fn default_plan_splits_mixed_request_plane_objectives() {
        let input = CreateSwarmObjectiveRunInput {
            objective: "Search GitHub docs and draft an email reply".to_string(),
            group_folder: "main".to_string(),
            chat_jid: "main".to_string(),
            created_by: "tester".to_string(),
            requested_lane: None,
            tasks: Vec::new(),
            max_concurrency: None,
        };
        let (tasks, dependencies) =
            build_default_swarm_plan(&input, "swarm-run-1", "2026-04-06T10:00:00Z");
        assert_eq!(tasks.len(), 4);
        assert_eq!(dependencies.len(), 4);
    }

    #[test]
    fn creates_and_pumps_swarm_run() -> Result<()> {
        let dir = tempdir()?;
        let mut app = NanoclawApp::open(test_config(dir.path()))?;
        let created = create_swarm_objective_run(
            &mut app,
            CreateSwarmObjectiveRunInput {
                objective: "Investigate the issue and verify the outcome".to_string(),
                group_folder: "main".to_string(),
                chat_jid: "main".to_string(),
                created_by: "tester".to_string(),
                requested_lane: None,
                tasks: Vec::new(),
                max_concurrency: Some(2),
            },
        )?;
        let executor = InProcessEchoExecutor::new("Andy");
        let summary = pump_swarm_once(&mut app, &executor)?;
        assert!(summary.tasks_claimed > 0);
        let details = get_swarm_run_details(&app.db, &created.run.id)?.unwrap();
        assert!(!details.tasks.is_empty());
        assert!(details.tasks.iter().any(|task| matches!(
            task.status,
            crate::foundation::SwarmTaskStatus::Completed
                | crate::foundation::SwarmTaskStatus::Ready
        )));
        Ok(())
    }
}
