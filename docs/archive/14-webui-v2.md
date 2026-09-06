WebUI v2（Phase 9）

本阶段目标：把现有原生 HTML+CSS+JS 的 `webui` crate（`ui/` 单文件）重构为 React + Vite 工程化方案，覆盖大部分 CLI 能力，并引入 WebSocket 实时通道。

非目标（明确推迟）：

- 角色卡导入 / 删除 UI（v2.4）
- 插件管理面板（v2.2）
- LLM 调用日志（v2.3）
- WebUI 内沙盒测试对话（v2.5）
- 主题切换 UI / 移动端响应式
- 多用户 / 权限分层（开源仓库被克隆使用，每个部署仍是单机单 Token 单管理员）

参考调研：`docs/_research/webui-modern-implementations.md`（NapCat / MaiBot / AstrBot / Koishi / NoneBot2 五项目横向对比）。

---

1. 范围与场景

- 用户：单用户（开发者本人）。开源性质意味着他人会克隆部署，每个部署实例仍只服务一人。
- 部署：仅 `127.0.0.1:8080`，HTTP 明文，不引入 HTTPS。
- 形态：桌面浏览器优先（视口宽度 ≥1024px）；小于该宽度给出"请使用桌面浏览器"提示。
- WebUI 不得成为 Core 依赖：维持现有 `webui` crate 独立进程，通过 UDS + Plugin API 拉取数据。

2. 技术栈

| 层 | 选型 | 备注 |
|---|---|---|
| 前端框架 | React 18 + Vite + TypeScript | 与 MaiBot 同源 |
| UI 组件 | shadcn/ui + Radix + Tailwind v4 | `components.json` 起步，`baseColor: slate`，覆盖 `--primary` 为 teal |
| 字体 | 系统字体栈 + JetBrains Mono（代码块） | 跨平台零字体加载成本 |
| 客户端状态 | Jotai | localStorage 持久化 auth atom 等 |
| 服务器状态 | TanStack Query | 普通 API 缓存与失效 |
| 路由 | TanStack Router | 类型化优先 |
| 表单 | react-hook-form + zod | 配置面板与状态编辑 |
| 代码编辑器 | CodeMirror 6 | 仅配置面板 toml 编辑 |
| 实时 | WebSocket（高频双向）+ TanStack Query 轮询/失效（普通 API） | MVP 引入 |
| 主题 | dark-only | CSS Token 集中管理 |
| 认证 | Bearer Token + localStorage | 长期有效，仅主动退出清除 |

3. 目录结构

```
yomua-bot/
├── webui/                       # Rust 后端（保留）
│   ├── src/
│   │   ├── api/                 # 现有 handler
│   │   ├── api_v2/              # 新增：配置 / 写操作 / schema
│   │   ├── ws/                  # 新增：WebSocket 通道
│   │   └── main.rs
│   └── Cargo.toml               # 加 axum-ws / tokio-stream / schemars 等
├── webui-frontend/              # 新增：独立前端工程
│   ├── src/
│   │   ├── components/
│   │   │   └── ui/              # shadcn 生成
│   │   ├── pages/
│   │   ├── providers/           # ErrorBoundary / Query / Theme / Animation / Router
│   │   ├── stores/              # Jotai atoms
│   │   ├── lib/
│   │   └── App.tsx
│   ├── components.json
│   ├── tailwind.config.ts
│   └── package.json
└── docs/
    ├── 13-roadmap.md
    └── 14-webui-v2.md           # 本文件
```

4. Provider 嵌套（照搬 MaiBot）

```
<StrictMode>
  <ErrorBoundary>
    <QueryClientProvider>
      <ThemeProvider>            // dark-only，预留 system hook
        <AnimationProvider>
          <RouterProvider>
            <Toaster />          // 全局 toast
          </RouterProvider>
        </AnimationProvider>
      </ThemeProvider>
    </QueryClientProvider>
  </ErrorBoundary>
</StrictMode>
```

5. 布局（CSS Token 对齐 MaiBot）

```
--layout-sidebar-width: 13rem   /* 折叠 4rem */
--layout-header-height: 3.5rem
--layout-content-max-width: 80rem
--visual-radius: 0.5rem
--color-primary: hsl(180 70% 45%)   /* teal accent */
```

物理结构：左侧 13rem 导航（可折叠到 4rem） + 顶栏 3.5rem + 主区（最大 80rem 居中）。

6. 页面清单

| # | 路径 | 页面 | 读 | 写 |
|---|---|---|:-:|:-:|
| 1 | `/login` | 登录页 | — | Token 提交 |
| 2 | `/` | 概览（默认） | Core 状态 / 绑定数 / 消息数 / 运行时间 / 最近事件 | — |
| 3 | `/characters` | 角色 | 表格（可排序可过滤） | — |
| 3a | Drawer | 角色详情 | 状态 / 绑定 / 消息 | 状态编辑（精力 / 注意力 / 压力） |
| 4 | `/bindings` | 会话绑定 | 表格 | 切换角色 |
| 5 | `/messages` | 消息流 | 时间倒序 + 过滤 | — |
| 6 | `/config` | 配置面板 | schema 渲染 + 实时校验 | 防抖 2s 自动保存 + 一键重启 |

7. 后端 API 新增 / 补齐

| 接口 | 用途 | 阶段 |
|---|---|---|
| `POST /api/bindings/{id}/switch` | 切换会话绑定角色（替代 `switch-character` CLI） | MVP |
| `GET /api/config/{file}/schema` | 暴露 toml 对应 JSON Schema | MVP |
| `PUT /api/config/{file}` | 更新 toml（带 schema 校验） | MVP |
| `POST /api/runtime/restart` | 重启运行时（写完配置后生效） | MVP |
| `GET /api/runtime/schema` | runtime.toml schema | MVP |
| `GET /api/llm/schema` | llm.toml schema | MVP |
| `GET /api/webui/schema` | webui.toml schema | MVP |
| `GET /ws` | WebSocket 升级握手 | MVP |
| `WS subscribe: status` | 状态变更推送（Core 连接、绑定数等） | MVP |
| `WS subscribe: messages` | 新消息推送 | MVP |
| `WS subscribe: state` | 角色状态变化推送 | MVP |
| `POST /api/characters/import` | 角色卡导入 | v2.4 |
| `DELETE /api/characters/{id}` | 角色删除 | v2.4 |
| `GET /api/plugins` | 列出插件 | v2.2 |
| `POST /api/plugins/{id}/toggle` | 启停插件 | v2.2 |
| `GET /api/llm/calls` | LLM 调用日志 | v2.3 |

8. 数据流（配置面板）

```
用户在表单修改字段
  └─ react-hook-form 监听
       └─ 防抖 2s
            └─ PUT /api/config/{file}  （zod schema 校验在后端再过一遍）
                 └─ 后端：serde + schemars 校验 → 写盘 → 内存 cache 更新
                      └─ 前端按钮「应用并重启」
                           └─ POST /api/runtime/restart
                                └─ Core 优雅重启
                                     └─ WebSocket broadcast `status` 事件
                                          └─ 前端 TanStack Query invalidate
```

9. 实时更新分层

| 场景 | 通道 | 理由 |
|---|---|---|
| 状态变更（Core 断连 / 绑定数变化） | WebSocket `subscribe: status` | 高频、低延迟 |
| 新消息 | WebSocket `subscribe: messages` | 高频、单向流 |
| 角色状态编辑后立刻看到 | WebSocket `subscribe: state` | 避免轮询 |
| 概览统计、列表加载 | TanStack Query（轮询 + 缓存） | 缓存复用更优 |
| 写操作结果反馈 | HTTP 响应 + Toast | 简单直接 |

10. 鉴权 UX

- 单 Token：源 `webui.toml::auth_token`，与现有实现一致。
- 前端：登录页提交 Token → 写入 `localStorage.yomua_auth_token`（Jotai atom 持久化）→ 跳转到 `/`。
- 所有 `fetch` 走统一拦截器：自动加 `Authorization: Bearer <token>`，收到 401 时清 atom + 跳 `/login`。
- 后端：`/api/**` 与 `/ws` 必须携带有效 Token，缺失/失效统一 401。

11. 设计 Token（CSS 变量）

五类 token，全部以 HSL 或具体数值声明在 `index.css`：

- `--color-*`：primary / accent / background / foreground / muted / destructive / border
- `--typography-*`：fontFamily / fontSize / fontWeight / lineHeight
- `--visual-*`：radius / shadow / borderWidth
- `--layout-*`：sidebarWidth / headerHeight / contentMaxWidth
- `--animation-*`：duration / easing

> 这套分类直接借鉴 MaiBot 的 `dashboard/src/index.css` 实现。

12. 分阶段交付

| 阶段 | 内容 | 验收 |
|---|---|---|
| 9.1 MVP | 登录 / 概览 / 角色（表格+Drawer+状态编辑）/ 绑定（含切换）/ 消息 / 配置面板（含重启） | 浏览器主路径手测 + Vitest 组件测试 + Rust 集成测试；四命令全绿 |
| 9.2 WebSocket | 实时通道接通：status / messages / state 推送 | 同上 |
| 9.3 插件面板 | 列出 / 启停 / 重载 | 同上 |
| 9.4 LLM 日志 | 调用详情 / Token / 延迟分页 | 同上 |
| 9.5 角色卡导入 / 删除 | 写入 / 校验 / 删除 | 同上 |
| 9.6 沙盒（可选） | WebUI 内测试对话，直接调 Core 对话管线 | 同上 |

13. 测试与验收

- 后端：`cargo test` 全绿，新增 handler 与 WS 通道集成测试覆盖 401 / 200 / 校验失败 / 重启链路。
- 前端：Vitest 覆盖关键组件（角色表格、Drawer、状态编辑、ConfigForm 的 schema 渲染与防抖保存）。
- 端到端（可选，9.1 不强求）：Playwright 跑登录 / 查看 / 编辑主路径。
- 项目级四命令：`check` / `test` / `clippy -D warnings` / `fmt --check`。

14. 实施批次（建议）

| 批次 | 内容 |
|---|---|
| 9.1.1 | 前端工程初始化（Vite + React + TS + shadcn + Tailwind + Provider 嵌套 + 路由骨架 + dark theme） |
| 9.1.2 | 登录页 + 鉴权拦截器 + 受保护路由 |
| 9.1.3 | 概览页（卡片 + 最近事件） |
| 9.1.4 | 角色表格 + Drawer + 状态编辑（PUT `/api/characters/{id}/state`） |
| 9.1.5 | 绑定表 + 切换角色（新增 POST `/api/bindings/{id}/switch`） |
| 9.1.6 | 消息流（过滤、分页） |
| 9.1.7 | 配置面板（schema 渲染 + 防抖保存 + 重启链路，含 3 个 toml 的 schema） |
| 9.1.8 | 移除旧 `ui/` 静态资源与相关 handler 引用 |
| 9.2 | WebSocket：握手 + 鉴权 + 订阅路由 + 前端 hook |
| 9.3 | 插件面板 |
| 9.4 | LLM 日志 |
| 9.5 | 角色卡导入 / 删除 |
| 9.6 | 沙盒 |

每批结束跑四命令全绿。

15. 风险与边界

- **Core 不能感知 WebUI**：WebUI 通过现有 Plugin API UDS 拉数据，新增 API 必须基于 Core 已暴露的能力；若 Core 缺能力，先扩 Core（独立 PR），不在 WebUI 中伪造数据。
- **配置面板的 schema 必须是真相之源**：后端用 `schemars` 从 serde 结构体派生，前端只渲染、不持有副本，避免漂移。
- **重启不可阻塞**：`POST /api/runtime/restart` 必须异步触发并立即返回 202，重启结果通过 WebSocket 推送。
- **暗色优先不引入偏好切换 UI**：本次仅 dark-only，但 CSS Token 结构保留 light 切换能力，便于未来加。

16. 后续可考虑（不在本阶段）

- 主题切换 UI（dark + system + 未来 light）
- 移动端响应式（<1024px 全屏 Dialog 替代 Drawer）
- PWA / 离线缓存
- WebUI 内沙盒测试对话（Koishi 借鉴）
- 角色关系图谱可视化（MaiBot 用 ReactFlow + dagre）
- 知识图谱 / 表达方式 / 表情包等资源管理
