import { SocketClient } from "nacket-sdk";
import { socketIo } from "nacket-sdk/socketio";

/** 当前页面拥有的唯一连接，重建前必须释放。 */
let client;
/** 页面仅保留有限展示记录，不保存 token 或完整正文。 */
const entries = [];

/** 业务作用：取得页面固定控件，不将业务内容当作选择器。
 * 参数说明：
 * - id：静态控件 ID。
 * 返回：页面已有元素。
 */
function element(id) {
  return document.getElementById(id);
}

/** 业务作用：以纯文本显示有限记录，避免业务正文作为 HTML 执行。
 * 参数说明：
 * - value：有限公开状态或回执投影。
 * 返回：最多保留 100 条展示行。
 */
function record(value) {
  entries.push(
    JSON.stringify(value, (_, item) => (typeof item === "bigint" ? item.toString() : item)).slice(
      0,
      2048,
    ),
  );
  if (entries.length > 100) {
    entries.shift();
  }
  element("events").textContent = entries.join("\n");
}

/** 业务作用：显示服务端冻结的只读定义和已确认订阅，不用页面输入推断权限。
 * 参数说明: 无。
 * 返回：未就绪时清空定义，订阅只展示 SDK 当前已确认集合。
 */
function showRoute() {
  const route = client?.routeInfo;
  element("definition").textContent = route
    ? JSON.stringify({
        routeId: route.routeId,
        routeRevision: route.routeRevision,
        serialization: route.serialization,
        clientMessages: route.clientMessages,
        notifications: route.notifications,
      })
    : "—";
  element("subscriptions").textContent = JSON.stringify(client?.subscriptions ?? []);
}

/** 业务作用：将页面动作失败转为有限公开提示，不输出异常正文或 token。
 * 参数说明：
 * - action：需要等待的页面操作。
 * 返回：操作完成；错误仅显示 SDK 固定码。
 */
async function invoke(action) {
  try {
    await action();
  } catch (error) {
    record({
      error: typeof error?.code === "string" ? error.code : "request_failed",
    });
  } finally {
    showRoute();
  }
}

element("connect").onclick = () =>
  invoke(async () => {
    client?.close();
    const token = element("token").value;
    element("token").value = "";
    const compatible = element("adapter").value === "socketio";
    client = new SocketClient({
      adapter: compatible ? socketIo : "native_websocket",
      url: element("url").value,
      routeId: element("route").value,
      endpoint: element("endpoint").value,
      namespace: element("endpoint").value,
      path: element("path").value,
      allowPlaintext: element("plaintext").checked,
      token: token || null,
    });
    client.onStatus((status) => {
      element("state").textContent = JSON.stringify(status);
      const ready = status.state === "ready";
      element("connect").disabled = !["idle", "closed", "disposed"].includes(status.state);
      element("sub").disabled = !ready || !client.routeInfo?.subscriptions;
      element("unsub").disabled = !ready;
      element("notify").disabled = !ready || !client.routeInfo?.notifications.includes("status");
      showRoute();
    });
    client.onState(showRoute);
    client.onMessage((message) => {
      record({
        id: message.id,
        event: message.event,
        bytes: message.raw?.byteLength ?? 0,
      });
      message.ack();
    });
    await client.connect();
  });
element("disconnect").onclick = () => client?.disconnect();
element("sub").onclick = () => invoke(async () => record(await client.sub(element("group").value)));
element("unsub").onclick = () =>
  invoke(async () => record(await client.unsub(element("group").value)));
element("notify").onclick = () => invoke(async () => record(await client.notify("status")));
element("publish").onclick = () =>
  invoke(async () => {
    const url = new URL(element("rest").value);
    if (url.protocol !== "https:" && !(element("plaintext").checked && url.protocol === "http:")) {
      throw new Error("invalid_target");
    }
    const response = await fetch(url, {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: element("body").value,
      signal: AbortSignal.timeout(10000),
    });
    record({
      restStatus: response.status,
      accepted: response.ok,
    });
  });
element("clear").onclick = () => {
  entries.length = 0;
  element("events").textContent = "";
};
window.addEventListener("pagehide", () => client?.close(), { once: true });
