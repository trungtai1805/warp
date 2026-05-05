use std::sync::Arc;

use ai::api_keys::OpenAICompatibleConfig;
use futures_util::StreamExt as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;
use warp_multi_agent_api as api;

use crate::ai::agent::AIAgentInput;
use crate::server::server_api::AIApiError;

use super::{ConvertToAPITypeError, RequestParams, ResponseStream};

const SYSTEM_PROMPT: &str = "You are replying inside Warp Agent through a local OpenAI-compatible endpoint. Provide useful terminal and coding help in plain text. This local compatibility mode cannot run commands, edit files, or call tools directly.";

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ChatCompletionMessage {
    role: String,
    content: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatCompletionMessage>,
    stream: bool,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatCompletionChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionChoice {
    message: ChatCompletionResponseMessage,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponseMessage {
    content: Value,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: Option<ErrorBody>,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ClientActionContext {
    task_id: String,
    request_id: String,
    create_task: bool,
    task_description: String,
}

pub fn should_use(params: &RequestParams) -> bool {
    params.openai_compatible_config.is_some() && latest_user_query(params).is_some()
}

pub async fn generate_multi_agent_output(
    params: RequestParams,
    cancellation_rx: futures::channel::oneshot::Receiver<()>,
) -> Result<ResponseStream, ConvertToAPITypeError> {
    let Some(config) = params.openai_compatible_config.clone() else {
        return Ok(empty_error_stream(
            "OpenAI-compatible endpoint is not configured.",
        ));
    };
    let Some(request) = build_chat_completion_request(&params, &config) else {
        return Ok(empty_error_stream(
            "OpenAI-compatible endpoint only supports text user queries.",
        ));
    };

    let conversation_id = params
        .conversation_token
        .as_ref()
        .map(|token| token.as_str().to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let request_id = Uuid::new_v4().to_string();
    let run_id = params
        .ambient_agent_task_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let task_id = params
        .tasks
        .first()
        .map(|task| task.id.clone())
        .filter(|task_id| !task_id.is_empty())
        .unwrap_or_else(|| run_id.clone());
    let client_action_context = ClientActionContext {
        task_id,
        request_id: request_id.clone(),
        create_task: params.tasks.is_empty(),
        task_description: latest_user_query(&params).unwrap_or_default(),
    };

    let stream = async_stream::stream! {
        yield Ok(init_event(conversation_id, request_id.clone(), run_id));

        match request_chat_completion(&config, &request).await {
            Ok(text) => {
                yield Ok(client_actions_event(&client_action_context, text));
                yield Ok(finished_event(api::response_event::stream_finished::Reason::Done(
                    api::response_event::stream_finished::Done {},
                )));
            }
            Err(message) => {
                yield Ok(finished_event(api::response_event::stream_finished::Reason::InternalError(
                    api::response_event::stream_finished::InternalError { message },
                )));
            }
        }
    }
    .take_until(cancellation_rx);

    Ok(Box::pin(stream))
}

fn empty_error_stream(message: &'static str) -> ResponseStream {
    let event = Err(Arc::new(AIApiError::Other(anyhow::anyhow!(message))));
    Box::pin(futures::stream::once(async move { event }))
}

async fn request_chat_completion(
    config: &OpenAICompatibleConfig,
    request: &ChatCompletionRequest,
) -> Result<String, String> {
    let Some(base_url) = config.base_url.as_deref() else {
        return Err("OpenAI-compatible endpoint is missing a base URL.".to_string());
    };

    let client = reqwest::Client::new();
    let mut request_builder = client.post(chat_completions_url(base_url)).json(request);
    if let Some(api_key) = config.api_key.as_deref().filter(|key| !key.is_empty()) {
        request_builder = request_builder.bearer_auth(api_key);
    }

    let response = request_builder
        .send()
        .await
        .map_err(|err| format!("OpenAI-compatible endpoint request failed: {err:#}"))?;

    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|err| format!("OpenAI-compatible endpoint response read failed: {err:#}"))?;

    if !status.is_success() {
        let message = serde_json::from_str::<ErrorResponse>(&body)
            .ok()
            .and_then(|response| response.error)
            .and_then(|error| error.message)
            .unwrap_or(body);
        return Err(format!(
            "OpenAI-compatible endpoint returned {status}: {message}"
        ));
    }

    let response = serde_json::from_str::<ChatCompletionResponse>(&body).map_err(|err| {
        format!("OpenAI-compatible endpoint response was not valid JSON: {err:#}")
    })?;
    response
        .choices
        .first()
        .and_then(|choice| content_to_text(&choice.message.content))
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| "OpenAI-compatible endpoint returned an empty response.".to_string())
}

fn build_chat_completion_request(
    params: &RequestParams,
    config: &OpenAICompatibleConfig,
) -> Option<ChatCompletionRequest> {
    let mut messages = Vec::with_capacity(1 + params.tasks.len() + params.input.len());
    messages.push(ChatCompletionMessage {
        role: "system".to_string(),
        content: SYSTEM_PROMPT.to_string(),
    });

    for task in &params.tasks {
        for message in &task.messages {
            if let Some(message) = chat_message_from_api_message(message) {
                messages.push(message);
            }
        }
    }

    messages.push(ChatCompletionMessage {
        role: "user".to_string(),
        content: latest_user_query(params)?,
    });

    Some(ChatCompletionRequest {
        model: config
            .model
            .clone()
            .unwrap_or_else(|| params.model.as_str().to_string()),
        messages,
        stream: false,
    })
}

fn chat_message_from_api_message(message: &api::Message) -> Option<ChatCompletionMessage> {
    match message.message.as_ref()? {
        api::message::Message::UserQuery(query) => Some(ChatCompletionMessage {
            role: "user".to_string(),
            content: query.query.clone(),
        }),
        api::message::Message::AgentOutput(output) => Some(ChatCompletionMessage {
            role: "assistant".to_string(),
            content: output.text.clone(),
        }),
        api::message::Message::SystemQuery(query) => {
            system_query_text(query).map(|content| ChatCompletionMessage {
                role: "user".to_string(),
                content,
            })
        }
        _ => None,
    }
}

fn system_query_text(query: &api::message::SystemQuery) -> Option<String> {
    match query.r#type.as_ref()? {
        api::message::system_query::Type::AutoCodeDiff(query) => Some(query.query.clone()),
        api::message::system_query::Type::CreateNewProject(query) => Some(query.query.clone()),
        api::message::system_query::Type::CloneRepository(query) => {
            Some(format!("Clone {}", query.url))
        }
        api::message::system_query::Type::SummarizeConversation(query) => {
            Some(query.prompt.clone())
        }
        _ => None,
    }
}

fn latest_user_query(params: &RequestParams) -> Option<String> {
    params.input.iter().rev().find_map(AIAgentInput::user_query)
}

fn chat_completions_url(base_url: &str) -> String {
    let base_url = base_url.trim().trim_end_matches('/');
    if base_url.ends_with("/chat/completions") {
        base_url.to_string()
    } else {
        format!("{base_url}/chat/completions")
    }
}

fn content_to_text(content: &Value) -> Option<String> {
    match content {
        Value::String(text) => Some(text.clone()),
        Value::Array(parts) => {
            let text = parts
                .iter()
                .filter_map(|part| match part {
                    Value::String(text) => Some(text.as_str()),
                    Value::Object(object) => object.get("text").and_then(Value::as_str),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("");
            (!text.is_empty()).then_some(text)
        }
        _ => None,
    }
}

fn init_event(conversation_id: String, request_id: String, run_id: String) -> api::ResponseEvent {
    api::ResponseEvent {
        r#type: Some(api::response_event::Type::Init(
            api::response_event::StreamInit {
                conversation_id,
                request_id,
                run_id,
            },
        )),
    }
}

fn client_actions_event(context: &ClientActionContext, text: String) -> api::ResponseEvent {
    let mut actions = Vec::new();
    if context.create_task {
        actions.push(api::ClientAction {
            action: Some(api::client_action::Action::CreateTask(
                api::client_action::CreateTask {
                    task: Some(api::Task {
                        id: context.task_id.clone(),
                        description: context.task_description.clone(),
                        dependencies: None,
                        messages: vec![],
                        summary: String::new(),
                        server_data: String::new(),
                    }),
                },
            )),
        });
    }

    actions.push(api::ClientAction {
        action: Some(api::client_action::Action::AddMessagesToTask(
            api::client_action::AddMessagesToTask {
                task_id: context.task_id.clone(),
                messages: vec![api::Message {
                    id: Uuid::new_v4().to_string(),
                    task_id: context.task_id.clone(),
                    request_id: context.request_id.clone(),
                    timestamp: None,
                    server_message_data: String::new(),
                    citations: vec![],
                    message: Some(api::message::Message::AgentOutput(
                        api::message::AgentOutput { text },
                    )),
                }],
            },
        )),
    });

    api::ResponseEvent {
        r#type: Some(api::response_event::Type::ClientActions(
            api::response_event::ClientActions { actions },
        )),
    }
}

fn finished_event(reason: api::response_event::stream_finished::Reason) -> api::ResponseEvent {
    api::ResponseEvent {
        r#type: Some(api::response_event::Type::Finished(
            api::response_event::StreamFinished {
                token_usage: vec![],
                should_refresh_model_config: false,
                request_cost: None,
                conversation_usage_metadata: None,
                reason: Some(reason),
            },
        )),
    }
}

#[cfg(test)]
mod tests {
    use ai::api_keys::OpenAICompatibleConfig;
    use serde_json::json;
    use warp_multi_agent_api as api;

    use crate::ai::agent::api::RequestParams;
    use crate::ai::agent::AIAgentInput;
    use crate::ai::blocklist::SessionContext;
    use crate::ai::llms::LLMId;

    use super::{
        build_chat_completion_request, chat_completions_url, client_actions_event, content_to_text,
        ClientActionContext,
    };

    fn request_params(input: Vec<AIAgentInput>, tasks: Vec<api::Task>) -> RequestParams {
        let model = LLMId::from("warp-model");
        RequestParams {
            input,
            conversation_token: None,
            forked_from_conversation_token: None,
            ambient_agent_task_id: None,
            tasks,
            existing_suggestions: None,
            metadata: None,
            session_context: SessionContext::new_for_test(),
            model: model.clone(),
            coding_model: model.clone(),
            cli_agent_model: model.clone(),
            computer_use_model: model,
            is_memory_enabled: false,
            warp_drive_context_enabled: false,
            context_window_limit: None,
            mcp_context: None,
            planning_enabled: true,
            should_redact_secrets: false,
            api_keys: None,
            openai_compatible_config: None,
            allow_use_of_warp_credits_with_byok: false,
            autonomy_level: api::AutonomyLevel::Supervised,
            isolation_level: api::IsolationLevel::None,
            web_search_enabled: false,
            computer_use_enabled: false,
            ask_user_question_enabled: false,
            research_agent_enabled: false,
            orchestration_enabled: false,
            supported_tools_override: None,
            parent_agent_id: None,
            agent_name: None,
        }
    }

    fn user_query(query: &str) -> AIAgentInput {
        AIAgentInput::UserQuery {
            query: query.to_string(),
            context: Default::default(),
            static_query_type: None,
            referenced_attachments: Default::default(),
            user_query_mode: Default::default(),
            running_command: None,
            intended_agent: None,
        }
    }

    #[test]
    fn builds_chat_completions_url_from_base_url() {
        assert_eq!(
            chat_completions_url("http://localhost:8080/v1/"),
            "http://localhost:8080/v1/chat/completions"
        );
        assert_eq!(
            chat_completions_url("http://localhost:8080/v1/chat/completions"),
            "http://localhost:8080/v1/chat/completions"
        );
    }

    #[test]
    fn builds_request_with_history_and_model_override() {
        let task = api::Task {
            id: "task-1".to_string(),
            description: String::new(),
            dependencies: None,
            messages: vec![api::Message {
                id: "message-1".to_string(),
                task_id: "task-1".to_string(),
                request_id: String::new(),
                timestamp: None,
                server_message_data: String::new(),
                citations: vec![],
                message: Some(api::message::Message::AgentOutput(
                    api::message::AgentOutput {
                        text: "Previous answer".to_string(),
                    },
                )),
            }],
            summary: String::new(),
            server_data: String::new(),
        };
        let params = request_params(vec![user_query("Next question")], vec![task]);
        let config = OpenAICompatibleConfig {
            base_url: Some("http://localhost:8080/v1".to_string()),
            api_key: None,
            model: Some("gpt-local".to_string()),
        };

        let request = build_chat_completion_request(&params, &config).unwrap();

        assert_eq!(request.model, "gpt-local");
        assert_eq!(request.messages[1].role, "assistant");
        assert_eq!(request.messages[1].content, "Previous answer");
        assert_eq!(request.messages.last().unwrap().content, "Next question");
    }

    #[test]
    fn extracts_text_from_string_and_part_array_content() {
        assert_eq!(
            content_to_text(&json!("plain text")),
            Some("plain text".to_string())
        );
        assert_eq!(
            content_to_text(&json!([
                { "type": "text", "text": "hello " },
                { "type": "text", "text": "world" }
            ])),
            Some("hello world".to_string())
        );
    }

    #[test]
    fn creates_task_when_no_existing_task_is_present() {
        let event = client_actions_event(
            &ClientActionContext {
                task_id: "task-1".to_string(),
                request_id: "request-1".to_string(),
                create_task: true,
                task_description: "Hello".to_string(),
            },
            "Hi".to_string(),
        );
        let Some(api::response_event::Type::ClientActions(actions)) = event.r#type else {
            panic!("expected client actions event");
        };

        assert_eq!(actions.actions.len(), 2);
        assert!(matches!(
            actions.actions[0].action,
            Some(api::client_action::Action::CreateTask(_))
        ));
        assert!(matches!(
            actions.actions[1].action,
            Some(api::client_action::Action::AddMessagesToTask(_))
        ));
    }
}
