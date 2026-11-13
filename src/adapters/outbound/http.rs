//! 固定远程入口的受管调用、容量和参数映射，业务授权结果由调用方验证。

use crate::{
    config::{Parameter, RemoteEndpoint, RemoteMethod, RemoteTarget},
    domain::message::{envelope::identifier, json},
};
use anyhow::Context;
use nasa::discovery::rest::Method;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{sync::Semaphore, time::Instant};

/// 复用 Application 的 HTTP 和发现能力，只持有产品调用预算，不创建新 HTTP 客户端。
pub struct RemoteCalls {
    /// 统一 HTTP 客户端和注册发现组件的宿主。
    application: nasa::Application,
    /// 节点远程调用数量及临时 JSON 空间的固定上限。
    policy: crate::config::remote_calls::RemoteCalls,
    /// 全部端点共享的节点调用数量池，等待端点容量期间也占额。
    total_calls: Arc<Semaphore>,
    /// 全部调用共用的请求、映射及响应保守临时字节池。
    total_bytes: Arc<Semaphore>,
    /// 会话握手上下文的独立长期保留预算，不借用 HTTP 请求的瞬时额度。
    context_bytes: Arc<Semaphore>,
    /// 固定 HTTP 调用事实及有限端点细分指标源。
    metrics: Arc<crate::observability::Metrics>,
    /// 以完整端点摘要区分的并发池弱引用，旧端点责任结束后可回收。
    budgets: Mutex<BTreeMap<[u8; 32], Weak<Semaphore>>>,
}

/// 调用失败携带提交边界；只有尚未移交 HTTP 执行器才能确认本次没有远端副作用。
pub struct CallFailure {
    /// 是否已经尝试交给传输层，决定有副作用调用能否明确报告未发出。
    pub dispatched: bool,
    /// 内部失败原因，不能原样暴露为含凭据或远程细节的客户端响应。
    pub cause: anyhow::Error,
}

impl RemoteCalls {
    /// 业务作用：通过受管 HTTP 连接池读取部署固定的公钥文档，不转发客户端凭据或接受响应跳转。
    /// 参数说明：
    /// - `client_ref`：部署已登记的 HTTP 客户端。
    /// - `url`：编译时已校验的公钥地址。
    /// - `deadline`：调用者剩余的绝对截止点。
    ///
    /// 返回：严格、有界 JSON 文档；错误不产生新密钥快照。
    pub async fn document(
        &self,
        client_ref: &str,
        url: &str,
        deadline: Instant,
    ) -> anyhow::Result<Value> {
        tokio::time::timeout_at(deadline, async {
            let _call = self.total_calls.clone().acquire_owned().await?;
            let _observed = self.metrics.reserve("remote_calls", 1);
            let size = 65536 * 32 + 4096;
            anyhow::ensure!(size <= self.policy.max_bytes, "remote_buffer_capacity");
            let _bytes = self
                .total_bytes
                .clone()
                .acquire_many_owned(size as u32)
                .await?;
            let _observed_bytes = self.metrics.reserve("remote_bytes", size as u64);
            let mut response = self
                .application
                .rest_client(client_ref)
                .await?
                .external_request(Method::GET, url)
                .timeout(deadline.saturating_duration_since(Instant::now()))
                .send_managed()
                .await?;
            anyhow::ensure!(response.status().is_success(), "public keys unavailable");
            let mut bytes = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .context("remote_transport_unavailable")?
            {
                anyhow::ensure!(
                    chunk.len() <= 65_536usize.saturating_sub(bytes.len()),
                    "public keys capacity"
                );
                bytes.extend_from_slice(&chunk);
            }
            json::decode(&bytes, 65_536)
        })
        .await?
    }
    /// 业务作用：将产品端点调用接入既有应用出站生命周期，共享同一连接池和发现目录。
    /// 参数说明：
    /// - `application`：napp 管理的应用句柄。
    /// - `policy`：节点固定的总并发、缓冲及请求大小上限。
    /// - `metrics`：共用有限资源观察。
    ///
    /// 返回：空端点目录及全节点容量，不创建 watcher、连接池或后台任务。
    pub fn new(
        application: nasa::Application,
        policy: crate::config::remote_calls::RemoteCalls,
        metrics: Arc<crate::observability::Metrics>,
    ) -> Self {
        Self {
            application,
            total_calls: Arc::new(Semaphore::new(policy.max_in_flight)),
            total_bytes: Arc::new(Semaphore::new(policy.max_bytes)),
            context_bytes: Arc::new(Semaphore::new(policy.max_context_bytes)),
            policy,
            metrics,
            budgets: Mutex::new(BTreeMap::new()),
        }
    }

    /// 业务作用：在保存会话附加材料前取得节点字节额度，容量不足拒绝新接入。
    /// 参数说明：
    /// - `bytes`：已按 DOM 开销保守计算的长期保留字节数。
    ///
    /// 返回：随会话释放的独占额度；不等待容量而延长握手期限。
    pub fn retain_context(
        &self,
        bytes: usize,
    ) -> anyhow::Result<tokio::sync::OwnedSemaphorePermit> {
        Ok(self
            .context_bytes
            .clone()
            .try_acquire_many_owned(u32::try_from(bytes)?)?)
    }

    /// 业务作用：在统一截止时间和响应容量内调用固定接口，拒绝重定向结果和模糊 JSON。
    /// 参数说明：
    /// - `endpoint_ref`：冻结计划中的受权端点名称，只用于有限指标目录。
    /// - `endpoint`：当前路由已冻结的部署端点。
    /// - `request`：由业务流程构造的可信上下文和受限参数。
    /// - `deadline`：调用者的会话或操作截止点，排队也占用同一预算。
    ///
    /// 返回：严格类型响应；网络、状态、容量或结构异常均不产生授权结论。
    pub async fn call<Q: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        endpoint_ref: &str,
        endpoint: &RemoteEndpoint,
        request: &Q,
        deadline: Instant,
    ) -> anyhow::Result<R> {
        Box::pin(self.call_observed(endpoint_ref, endpoint, request, deadline))
            .await
            .map_err(|failure| failure.cause)
    }

    /// 业务作用：保留副作用请求的提交边界，避免把排队拒绝与远端结果未知混为一类。
    /// 参数说明：
    /// - `endpoint_ref`：冻结计划中的受权端点名称，只用于有限指标目录。
    /// - `endpoint`：冻结的调用策略和目标。
    /// - `request`：已构造的业务参数及关联身份。
    /// - `deadline`：排队、构造与网络等待共用的截止点。
    ///
    /// 返回：严格响应；失败同时说明请求是否已移交 HTTP 执行器，移交不等于远端已经执行。
    pub async fn call_observed<Q: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        endpoint_ref: &str,
        endpoint: &RemoteEndpoint,
        request: &Q,
        deadline: Instant,
    ) -> Result<R, CallFailure> {
        // 多种身份与操作共用此调用边界，独立保留 HTTP 状态避免上层分支叠加大型异步栈帧。
        Box::pin(self.call_payload_observed(endpoint_ref, endpoint, request, None, deadline)).await
    }

    /// 业务作用：在业务接口显式选择原始正文时保持二进制载荷，可信上下文进入固定有界 header。
    /// 参数说明：
    /// - `endpoint_ref`：冻结计划中的受权端点名称，只用于有限指标目录。
    /// - `endpoint`：冻结的目标和调用预算。
    /// - `request`：固定控制字段；有正文时不含凭据及业务对象。
    /// - `body`：原始业务字节；None 沿用普通 JSON 请求。
    /// - `deadline`：从动作接纳起固定的截止点。
    ///
    /// 返回：严格响应或带移交边界的失败，正文布局不会因远端错误自动切换。
    pub async fn call_payload_observed<Q: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        endpoint_ref: &str,
        endpoint: &RemoteEndpoint,
        request: &Q,
        body: Option<bytes::Bytes>,
        deadline: Instant,
    ) -> Result<R, CallFailure> {
        let mut observation = self.metrics.scopes.event(
            crate::observability::scopes::Kind::Endpoint,
            endpoint_ref,
            "request",
        );
        let deadline = deadline.min(Instant::now() + Duration::from_millis(endpoint.timeout_ms));
        let mut dispatched = false;
        let operation = async {
            let _call = self.total_calls.clone().acquire_owned().await?;
            let _observed = self.metrics.reserve("remote_calls", 1);
            let budget = self.budget(endpoint)?;
            let _permit = budget.acquire_owned().await?;
            // 先进行无正文分配的长度检查，再覆盖 JSON 构造、映射、编码及最大响应的并存成本。
            let request_size = serialized_size(request, self.policy.max_request_bytes)?;
            let mapping_size = endpoint.call.parameters.iter().try_fold(
                0usize,
                |total, (name, parameter)| -> anyhow::Result<usize> {
                    let value = match parameter {
                        Parameter::Constant {
                            value,
                        } => serialized_size(value, self.policy.max_request_bytes)?,
                        Parameter::Field {
                            max_bytes,
                            ..
                        } => *max_bytes,
                    };
                    Ok(total + value + name.len() * 6 + 32)
                },
            )?;
            let cost = (request_size + mapping_size + endpoint.max_response_bytes) * 32 + 4096;
            anyhow::ensure!(cost <= self.policy.max_bytes, "remote_buffer_capacity");
            let _bytes = self
                .total_bytes
                .clone()
                .acquire_many_owned(cost as u32)
                .await?;
            let _observed_bytes = self.metrics.reserve("remote_bytes", cost as u64);
            let mut request = serde_json::to_value(request)?;
            map_parameters(endpoint, &mut request)?;
            let encoded = serde_json::to_vec(&request)?;
            anyhow::ensure!(
                encoded.len() <= self.policy.max_request_bytes,
                "remote request capacity"
            );
            let method = match endpoint.call.method {
                RemoteMethod::Get => Method::GET,
                RemoteMethod::Post => Method::POST,
            };
            let mut builder = match &endpoint.target {
                RemoteTarget::Direct {
                    client_ref,
                    url,
                } => self
                    .application
                    .rest_client(client_ref)
                    .await?
                    .external_request(method, url),
                RemoteTarget::Discovery {
                    connection,
                    service,
                    path,
                } => self
                    .application
                    .discovery()?
                    .rest(Some(connection))?
                    .service_request(service, method, path),
            };
            if let Some(reference) = &endpoint.call.service_token_secret_ref {
                let secret = std::env::var(reference)
                    .map_err(|_| anyhow::anyhow!("remote service credential unavailable"))?;
                anyhow::ensure!(
                    !secret.is_empty()
                        && secret.len() <= 8192
                        && !secret.chars().any(char::is_control),
                    "invalid remote service credential"
                );
                builder = builder.header("authorization", format!("Bearer {secret}"));
            }
            builder = builder.timeout(deadline.saturating_duration_since(Instant::now()));
            builder = if let Some(body) = body {
                anyhow::ensure!(
                    endpoint.call.method == RemoteMethod::Post && encoded.len() <= 6144,
                    "raw_http_requires_bounded_post_context"
                );
                builder
                    .header("content-type", "application/octet-stream")
                    .header("nacket-context", nasa::crypto::base64_url_encode(&encoded))
                    .raw_body(body)
            } else if endpoint.call.method == RemoteMethod::Get {
                // GET 的 URL 不能携带用户 token，业务动作也不得借查询重试产生重复副作用。
                anyhow::ensure!(
                    request.get("credentials").is_none()
                        && matches!(
                            request.get("operation").and_then(Value::as_str),
                            Some("connect" | "subscribe" | "status" | "authorize")
                        ),
                    "GET requires a credential-free read operation"
                );
                let mut query = BTreeMap::new();
                for (key, value) in request
                    .as_object()
                    .ok_or_else(|| anyhow::anyhow!("remote request object required"))?
                {
                    query.insert(
                        key.clone(),
                        match value {
                            Value::String(value) => value.clone(),
                            _ => serde_json::to_string(value)?,
                        },
                    );
                }
                builder.query(&query)
            } else {
                builder
                    .header("content-type", "application/json")
                    .raw_body(encoded)
            };
            // 排队及参数映射失败尚无远端动作；从移交开始，响应丢失只能报告结果未知。
            anyhow::ensure!(Instant::now() < deadline, "remote_deadline");
            dispatched = true;
            let mut response = builder
                .send_managed()
                .await
                .context("remote_transport_unavailable")?;
            anyhow::ensure!(response.status().is_success(), "remote_http_unavailable");
            let mut bytes = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .context("remote_transport_unavailable")?
            {
                anyhow::ensure!(
                    chunk.len() <= endpoint.max_response_bytes.saturating_sub(bytes.len()),
                    "remote response capacity"
                );
                bytes.extend_from_slice(&chunk);
            }
            let raw: Value = json::decode(&bytes, endpoint.max_response_bytes)?;
            let normalized = map_response(endpoint, raw)?;
            Ok(serde_json::from_value(normalized)?)
        };
        let result = match tokio::time::timeout_at(deadline, operation).await {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        };
        observation.finish(
            result
                .as_ref()
                .map(|_| "ok")
                .unwrap_or_else(|error| crate::observability::outcome(error)),
        );
        result.map_err(|cause| CallFailure {
            dispatched,
            cause,
        })
    }

    /// 业务作用：共享同一冻结端点的并发额度，避免每个连接各建一份限流器。
    /// 参数说明：
    /// - `endpoint`：完整已冻结端点定义。
    ///
    /// 返回：该定义的共享信号量；目录容量不足时拒绝新增调用域。
    fn budget(&self, endpoint: &RemoteEndpoint) -> anyhow::Result<Arc<Semaphore>> {
        anyhow::ensure!(
            (1..=4096).contains(&endpoint.call.max_in_flight),
            "remote concurrency limit"
        );
        let key: [u8; 32] = Sha256::digest(serde_json::to_vec(endpoint)?).into();
        let mut budgets = self.budgets.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(budget) = budgets.get(&key).and_then(Weak::upgrade) {
            return Ok(budget);
        }
        budgets.retain(|_, budget| budget.strong_count() > 0);
        anyhow::ensure!(budgets.len() < 4096, "remote budget catalog capacity");
        let budget = Arc::new(Semaphore::new(endpoint.call.max_in_flight));
        budgets.insert(key, Arc::downgrade(&budget));
        Ok(budget)
    }
}

/// 业务作用：为固定接口补充有限参数，禁止映射覆盖服务端建立的身份及路由上下文。
/// 参数说明：
/// - `endpoint`：部署批准的参数来源和容量。
/// - `request`：待发送的标准请求，调用方负责可信与不可信字段来源。
///
/// 返回：parameters 已合并；缺失、重名或超长值在网络调用前拒绝。
fn map_parameters(endpoint: &RemoteEndpoint, request: &mut Value) -> anyhow::Result<()> {
    let mut parameters = serde_json::Map::new();
    for (key, mapping) in &endpoint.call.parameters {
        identifier(key)?;
        let value = match mapping {
            Parameter::Constant {
                value,
            } => value.clone(),
            Parameter::Field {
                pointer,
                required,
                max_bytes,
            } => {
                anyhow::ensure!(
                    [
                        "/context/",
                        "/principal/",
                        "/arguments/",
                        "/handshake/",
                        "/headers/"
                    ]
                    .iter()
                    .any(|prefix| pointer.starts_with(prefix))
                        && (1..=4096).contains(max_bytes),
                    "invalid parameter source"
                );
                let value = match request.pointer(pointer) {
                    Some(value) => value,
                    None if !required => continue,
                    None => anyhow::bail!("required remote parameter missing"),
                };
                anyhow::ensure!(
                    serde_json::to_vec(value)?.len() <= *max_bytes,
                    "remote parameter capacity"
                );
                value.clone()
            }
        };
        anyhow::ensure!(
            !value.is_array() && !value.is_object() && !value.is_null(),
            "remote parameters must be scalar"
        );
        parameters.insert(key.clone(), value);
    }
    if !parameters.is_empty() {
        let object = request
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("remote request object required"))?;
        let current = object
            .entry("parameters")
            .or_insert_with(|| Value::Object(Default::default()))
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("parameters object required"))?;
        for (key, value) in parameters {
            anyhow::ensure!(
                !current.contains_key(&key),
                "remote parameter mapping collision"
            );
            current.insert(key, value);
        }
    }
    Ok(())
}

/// 业务作用：把显式登记的旧接口字段映射到标准响应，不以 HTTP 成功或非空正文代表允许。
/// 参数说明：
/// - `endpoint`：固定响应字段和决策值映射。
/// - `response`：已通过重复键和容量检查的 JSON。
///
/// 返回：标准响应对象；未声明的远端动作不会自动成为执行指令。
fn map_response(endpoint: &RemoteEndpoint, response: Value) -> anyhow::Result<Value> {
    let mut normalized = if endpoint.call.response_fields.is_empty() {
        response
    } else {
        let mut fields = serde_json::Map::new();
        for (name, pointer) in &endpoint.call.response_fields {
            if let Some(value) = response.pointer(pointer) {
                fields.insert(name.clone(), value.clone());
            }
        }
        Value::Object(fields)
    };
    if let Some(decision) = normalized.get_mut("decision") {
        let value = decision
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("invalid decision type"))?;
        *decision = Value::String(
            if value == endpoint.call.allow_value {
                "allow"
            } else if value == endpoint.call.deny_value {
                "deny"
            } else {
                anyhow::bail!("unknown authorization decision")
            }
            .into(),
        );
    }
    Ok(normalized)
}

struct SizeCounter {
    /// 预编码遍历已累计的 JSON 字节数，不分配完整请求正文。
    used: usize,
    /// 此次编码允许的最大字节数，超限立即拒绝继续构造。
    maximum: usize,
}
impl std::io::Write for SizeCounter {
    /// 业务作用：仅计量序列化长度，在达到请求上限前终止，不创建待发送正文。
    /// 参数说明：
    /// - `bytes`：序列化器本次生成的片段。
    ///
    /// 返回：有限计量结果；超限以固定错误拒绝。
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.used) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "remote request capacity",
            ));
        }
        self.used += bytes.len();
        Ok(bytes.len())
    }
    /// 业务作用：长度检查不拥有输出介质，刷新不会产生远程副作用。
    /// 参数说明: 无。
    ///
    /// 返回：计量状态保持不变。
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
/// 业务作用：在 JSON 物化之前核对请求大小，保留尚未向 HTTP 移交的明确边界。
/// 参数说明：
/// - `value`：准备发送的受限控制请求或映射常量。
/// - `maximum`：单请求允许的最大编码字节数。
///
/// 返回：真实 JSON 长度；超限或序列化失败不创建业务请求。
fn serialized_size<Q: Serialize + ?Sized>(value: &Q, maximum: usize) -> anyhow::Result<usize> {
    let mut counter = SizeCounter {
        used: 0,
        maximum,
    };
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.used)
}
