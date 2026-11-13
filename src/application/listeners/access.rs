use super::*;
use crate::domain::control::access::{Endpoint, ListenerAccess};

impl Listeners {
    /// 业务作用：从本节点真实 owner 取得精确绑定身份，主监听器使用本次进程的固定身份。
    /// 参数说明：
    /// - `route`：已经冻结的路由定义。
    ///
    /// 返回：真实本地地址与 bindingId；不按其它节点的自动端口推测结果。
    pub fn bound(
        &self,
        route: &crate::domain::route::RouteDefinition,
    ) -> Option<(String, SocketAddr)> {
        let key = if route.access.dedicated_port.enabled {
            format!("route:{}", route.route_id)
        } else {
            format!("shared:{}", route.access.listener_ref)
        };
        if !route.access.dedicated_port.enabled
            && route.access.listener_ref == self.gateway.configuration.primary_listener
        {
            return Some((self.gateway.startup_id.clone(), self.primary().0));
        }
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let id = state.current.get(&key)?;
        let owner = state.all.get(id)?;
        Some((id.clone(), owner.server.local_addr))
    }

    /// 业务作用：把已绑定端口映射为部署明确准备的外部入口，仍须与当前许可共同提交后才可查询。
    /// 参数说明：
    /// - `route`：节点正在准备或续约的精确定义。
    ///
    /// 返回：公开开关关闭时为 None；缺失映射或真实 listener 时拒绝目录与许可更新。
    pub fn access(
        &self,
        route: &crate::domain::route::RouteDefinition,
    ) -> anyhow::Result<Option<ListenerAccess>> {
        let template = self
            .templates
            .get(&route.access.listener_ref)
            .ok_or_else(|| anyhow::anyhow!("listener_template_missing"))?;
        let advertise = &template.advertise;
        if !advertise.enabled {
            return Ok(None);
        }
        anyhow::ensure!(
            !self.closed.load(Ordering::Acquire),
            "listener_manager_closed"
        );
        let (binding_id, bound_address) = self
            .bound(route)
            .ok_or_else(|| anyhow::anyhow!("listener_binding_missing"))?;
        let port = advertise.public_port(bound_address.port())?;
        let settings = &self.gateway.configuration.transport;
        let mut endpoints = Vec::new();
        if let Some(endpoint) = route.access.native_endpoint() {
            if template.protocols.contains(&Protocol::NativeTcp) && settings.native_tcp {
                endpoints.push(Endpoint {
                    adapter: Protocol::NativeTcp,
                    scheme: if advertise.secure {
                        "tls"
                    } else {
                        "tcp"
                    }
                    .into(),
                    path: None,
                    endpoint: endpoint.into(),
                });
            }
            if template.protocols.contains(&Protocol::NativeWs) && settings.native_websocket {
                endpoints.push(Endpoint {
                    adapter: Protocol::NativeWs,
                    scheme: if advertise.secure {
                        "wss"
                    } else {
                        "ws"
                    }
                    .into(),
                    path: Some(settings.native_path.clone()),
                    endpoint: endpoint.into(),
                });
            }
        }
        if let Some(endpoint) = route.access.socketio_namespace() {
            if template.protocols.contains(&Protocol::Socketio) && settings.socketio {
                endpoints.push(Endpoint {
                    adapter: Protocol::Socketio,
                    scheme: if advertise.secure {
                        "https"
                    } else {
                        "http"
                    }
                    .into(),
                    path: Some(settings.socketio_path.clone()),
                    endpoint: endpoint.into(),
                });
            }
        }
        let access = ListenerAccess {
            binding_id,
            bound_address,
            host: advertise.host.clone(),
            port,
            secure: advertise.secure,
            endpoints,
        };
        access.validate()?;
        Ok(Some(access))
    }
}
