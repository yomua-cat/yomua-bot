# 现代中文 QQ Bot 项目的 WebUI 调研

> 为 yomua-bot（Rust QQ 角色 Bot）重构 WebUI 阶段的参考调研。  
> 目标：了解同类项目在布局、视觉风格、面板划分、鉴权与实时更新上的成熟做法，避免凭空造轮子。
>
> 本调研覆盖的项目（按用户给定的优先级）：
> 1. NapCatQQ（含新一代 GUI 替代 SnowLuma）
> 2. MaiBot / 麦麦
> 3. AstrBot
> 4. Koishi
> 5. NoneBot2（含 NoneBot Desktop）
>
> 所有引用均给出原始来源 URL；信息以「可观察 / 可验证」为限，未确认处明确标注。

---

## 0. 项目与 WebUI 现状速览

| 项目 | 主语言 / 协议 | 是否有自带 WebUI | 框架 / 风格 | 一句话定位 |
|---|---|---|---|---|
| NapCatQQ | TypeScript / NTQQ 协议 | 内置 `napcat-webui-frontend` + `napcat-webui-backend`（推荐新版用 SnowLuma 替代） | Vue 3 / Element Plus 风 | 协议端管理器，配置 OneBot v11/v12 转发 |
| SnowLuma | TypeScript / NTQQ | 自带（localhost:5099） | 未开源 UI；功能型 | NapCat 作者推荐的新一代 GUI |
| MaiBot / 麦麦 | Python / OneBot | `dashboard/` 完整子项目 | React 19 + shadcn/ui + Tailwind v4 | 数字生命体，AI 角色 QQ 群 Bot |
| AstrBot | Python / 多协议 | `dashboard/` 子项目 | Vue 3 + Vuetify 3 + Pinia + Monaco | LLM Agent 多平台 Bot 框架 |
| Koishi | TypeScript / Koishi 协议 | 内置 Console（多页） | 自研 + VuePress 文档站 | 跨平台 Bot 框架 |
| NoneBot2 | Python / OneBot 等多协议 | NoneBot Desktop（独立工具） | Electron 桌面端 | Python 异步 Bot 框架 |

> yomua-bot 当前 WebUI 是原生 HTML + 单文件 CSS/JS（`ui/`），运行在独立的 `webui` Rust 进程里。Phase 9 计划重构。

---

## 1. NapCatQQ / NapCat

### 仓库与现状

- 主仓库：<https://github.com/NapNeko/NapCatQQ>（10.5k★）
- 自带 WebUI 拆成两个包：
  - `packages/napcat-webui-frontend`（[目录](https://github.com/NapNeko/NapCatQQ/tree/main/packages/napcat-webui-frontend)）
  - `packages/napcat-webui-backend`（[目录](https://github.com/NapNeko/NapCatQQ/tree/main/packages/napcat-webui-backend)）
- 作者在 README 顶部明确写明：
  > 可以试试更新更好用的 [SnowLuma](https://github.com/SnowLuma/SnowLuma) 作为 NapCat Gui 替代品。
  
  → **官方 GUI 重心已迁移**，对 yomua-bot 的启发是：NapCat 这种"协议端"产品的 WebUI 是高度功能化的（账号状态 / 网络 / 日志 / 调试），不是花哨的角色管理。

### 视觉与交互（基于 README / 文档站描述，未深入其前端源码）

- 主色调：浅紫色 + 白色（NapCat 标志色）
- 文档站示例截图：<https://napneko.github.io/>
- UI 设计语言贴近 OneBot 生态的传统后台：表单 + 表格 + 实时日志流
- 主要功能面板（来自文档站「可视化管理工具」说明）：
  - 账号登录状态 / 二维码扫码
  - OneBot 服务端配置（WebSocket / HTTP）
  - 日志查看
  - 插件（QQ 侧 NapCat 插件）开关
  - 调试器（OneBot 动作测试）

### 新一代 GUI：SnowLuma（建议重点参考）

- 仓库：<https://github.com/SnowLuma/SnowLuma>（1.1k★，NapCat 同一作者生态）
- README 自描述（核心能力表）：
  > **WebUI 管理**：账号状态、实时日志、连接配置、动作调试和存储管理
- 启动方式：`launcher.bat` / `launcher.sh`，WebUI 默认监听 `http://localhost:5099`
- 鉴权方式：使用「启动日志中的初始密码」登录 WebUI（见 README 快速开始第 3 步）
- 运行时架构图（README 中的 `runtime-map.svg`）展示了清晰的层级：
  > QQ 会话 → 协议桥接 → OneBot 标准化 → WebSocket / HTTP / **WebUI** / SDK / MCP
- 视觉素材：<https://github.com/SnowLuma/SnowLuma/blob/main/assets/readme/hero.svg>

### 可借鉴要点

- **WebUI 作为外部进程独立运行**（SnowLuma 用 `launcher.*` 启动 WebUI），与 yomua-bot 当前 `webui` 作为独立 Rust 进程的设计契合。
- **鉴权用初始密码打印在启动日志**——简单、可工作、不引入新概念；适合本地/自托管场景。
- 协议端 WebUI 关注的是 **连接性 + 实时性 + 调试**，不是角色管理——我们应避免把 yomua-bot 的 WebUI 做成 NapCat 那种"功能堆叠"风格。

---

## 2. MaiBot / 麦麦（数字生命体 Bot）

> 这是与 yomua-bot 定位最接近的项目：**AI 角色 + QQ 群聊 + 自带 WebUI**。

### 仓库

- 主仓库：<https://github.com/Mai-with-u/MaiBot>（5.9k★，注意路径从 `SengokuCola/MaiMBot` 迁到 `Mai-with-u/MaiBot`）
- Dashboard 子项目：<https://github.com/Mai-with-u/MaiBot/tree/main/dashboard>
- Dashboard README：内容极其详细（每个面板都列了出来），是本次调研最有价值的资料。

### 技术栈（来自 `dashboard/package.json` 和 Dashboard README）

| 维度 | 选型 |
|---|---|
| 框架 | **React 19.2** + TypeScript 5.9 |
| 构建 | **Vite 7.2** |
| 路由 | **TanStack Router** |
| 状态 | **Jotai** + TanStack Query |
| 虚拟滚动 | TanStack Virtual（日志页） |
| UI 组件 | **shadcn/ui**（Radix UI 基础）+ lucide-react |
| 样式 | **Tailwind CSS 4.2** |
| 图表 | **Recharts** |
| 图谱可视化 | **ReactFlow** + dagre |
| 包管理 | 推荐 Bun，可 fallback 到 npm |
| 后端 | FastAPI（独立进程） |
| 实时通信 | **WebSocket**（日志 + 本地聊天） |
| 鉴权 | Token（可自定义或自动生成） |
| 主题 | **双主题**：modern（shadcn 默认风格） + future-retro（自定义复古机械风） |
| 桌面端 | 同时支持 Electron（`electron/` 目录） |

### 布局模式（来自 `dashboard/index.html` + `src/main.tsx`）

- 主入口在 `<div id="root">`，React 渲染
- 顶层 Provider 嵌套（main.tsx 中可观察到）：
  ```
  StrictMode
  → ErrorBoundary
    → QueryClientProvider
      → AnnouncerProvider（无障碍）
        → AssetStoreProvider
          → ThemeProvider（默认 theme="system"）
            → AnimationProvider
              → TourProvider（新手引导）
                → RouterProvider
                → Toaster（全局通知）
  ```
- 物理布局（在 Dashboard README 中明确说明：`src/components/layout.tsx`）：
  - 侧边栏（可折叠，宽度 13rem / 折叠后 4rem）+ 顶栏（高度 3.5rem）+ 主区域
  - 布局常量以 CSS 变量集中管理（见下文 CSS Token 节）

### 关键面板清单（Dashboard README 列出的全部模块）

1. **仪表盘（首页）**：总请求数、Token 消耗、费用、在线时长；模型统计；趋势折线图；模型使用饼图；最近活动列表
2. **本地聊天室**：WebSocket 实时通信 + SQLite 历史 + 自定义昵称 + 移动端适配
3. **配置管理**（核心创新点）：
   - 麦麦主程序配置：分组展示、自动生成表单、2 秒防抖自动保存、一键重启生效
   - AI 模型厂商：模板选择（OpenAI / DeepSeek / 硅基流动等）+ **连接测试按钮** + 批量操作 + 搜索
   - 模型管理：任务分配（回复 / 工具调用 / VLM 各自分配模型）+ 参数调整
   - 适配器配置（NapCat 适配）
4. **实时日志**：WebSocket 流式 + TanStack Virtual 虚拟滚动 + 多级过滤（DEBUG/INFO/WARN/ERROR + 模块 + 时间范围 + 关键字高亮 + 字号调整 + 导出）
5. **插件管理**：插件市场 + 分类筛选 + 一键安装（WebSocket 显示进度）+ 版本兼容性检查
6. **人物关系管理**：用户列表 + 详情编辑 + 互动统计 + 批量删除
7. **资源管理**：表情包、表达方式、知识图谱（ReactFlow 可视化）
8. **系统设置**：主题切换（亮/暗/跟随系统）、动画开关、Token 管理、版本信息

### 视觉风格（来自 `dashboard/src/index.css`）

- 使用 Tailwind CSS v4 的 `@theme inline` + HSL CSS 变量集中管理所有颜色 token：
  - `--color-primary` (HSL: 28.9 94.8% 45.1% —— 橙红色)
  - `--color-accent` (HSL: 112.7 40.2% 47.8% —— 草绿)
  - 支持 `.dark` 类切换
- 设计 token 分三类：
  - Color Tokens（`--color-*`）
  - Typography Tokens（`--typography-*`）
  - Visual Tokens（`--visual-radius-*`、`--visual-shadow-*`）
  - Layout Tokens（`--layout-sidebar-width` 等）
  - Animation Tokens（`--animation-anim-duration-*`）
- 字体：
  - 基础：`-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, ...`
  - 代码：`JetBrains Mono`
- **未来复古主题（future-retro）**：自定义的复古工业风格，色板：
  - 墨绿主色 `#0a4550`
  - 铁锈橙 `#c24d24`
  - 暖纸黄 `#c99a3e`
  - 圆角强制 `0`，按钮带刻线边框，状态点改为方块（这是显著的设计差异化）
- 双重主题通过 `localStorage` 持久化，支持 system 模式自动切换：
  ```js
  const mode = localStorage.getItem('maibot-theme-mode') ?? 'system';
  const theme = mode === 'system' ? matchMedia('(prefers-color-scheme: dark)') : mode;
  ```

### 鉴权 UX

- **Token 机制**：自定义或自动生成（Dashboard README 中明确写出）
- WebUI 登录页（`src/routes/auth.tsx`）——文件路径证实有独立登录页
- 封装 `fetch-with-auth.ts` 统一处理认证请求
- 状态用 Jotai atom 持久化（`src/store/auth.ts`）

### 实时更新模式

- **WebSocket** 是主力：日志流、聊天消息、安装进度
- **TanStack Query** 用于普通 API 缓存（仪表盘统计、模型列表等）
- 文档明确说"配置自动保存"，2 秒防抖

### 截图

- 主截图：<https://github.com/Mai-with-u/MaiBot/raw/main/dashboard/docs/main.png>
- WebUI 总览：<https://github.com/Mai-with-u/MaiBot/raw/main/depends-data/webui-showcase.jpg>
- 角色形象：<https://github.com/Mai-with-u/MaiBot/raw/main/depends-data/maimai-v2.png>

### 可借鉴要点

1. **dashboard 作为独立子项目**，与后端解耦——yomua-bot 当前 `webui` crate + `ui/` 静态文件结构类似，可以演化成同款 monorepo 风格。
2. **Provider 嵌套分层**（错误边界 / Query / 主题 / 动画 / 引导 / 路由 / Toast）——是一个 React + shadcn 项目可参考的成熟组织方式。
3. **设计 token 用 CSS 变量集中管理**，分颜色 / 排版 / 视觉 / 布局 / 动画五类——比直接用 Tailwind 工具类更可控。
4. **配置面板自动生成**：解析后端 dataclass/schema 自动渲染表单 + 防抖自动保存。这正是 yomua-bot 需要的"配置管理"形态（替代手动改 TOML）。
5. **WebSocket + TanStack Virtual** 做日志页：可处理万行级别日志不卡顿。
6. **双主题切换（含 system 模式）**——用户原计划"dark-only"，但应该至少保留 system 选项，避免与 macOS / Windows 系统主题冲突的视觉割裂感。

---

## 3. AstrBot

### 仓库

- 主仓库：<https://github.com/AstrBotDevs/AstrBot>（40k★，从 `Soulter/AstrBot` 迁到 `AstrBotDevs/AstrBot`）
- Dashboard 子项目：<https://github.com/AstrBotDevs/AstrBot/tree/master/dashboard>
- 文档站：<https://docs.astrbot.app/>（中文友好）
- 桌面端：`AstrBot-desktop`（独立仓库）

### 技术栈（来自 `dashboard/package.json`）

| 维度 | 选型 |
|---|---|
| 框架 | **Vue 3.3** + TypeScript |
| 构建 | Vite 6 |
| UI 组件 | **Vuetify 3.7**（Material Design 风格） |
| 路由 | vue-router 4 |
| 状态 | **Pinia 2** |
| 编辑器 | **Monaco Editor**（配置编辑） + `@guolao/vue-monaco-editor` |
| 图表 | **ApexCharts**（`vue3-apexcharts`） |
| 富文本 | **TipTap 2** + katex + markdown-it + mermaid |
| 高亮 | **Shiki** |
| 国际化 | vue-i18n |
| 字体 | Outfit + Noto Sans（Google Fonts） |
| 后端 API | FastAPI（自动生成 OpenAPI 客户端，用 `@hey-api/openapi-ts`） |
| **模板来源** | 基于 **CodedThemes Berry**（免费 Vue 后台模板） |

### 布局与视觉

- 走 **Material Design** 风（Vuetify 3 默认），与 MaiBot 的 shadcn 风形成鲜明对比
- 主色：`PurpleTheme` / `PurpleThemeDark`（紫调），可通过 localStorage 自定义 primary / secondary
- 主题模式：light / dark / system，通过 Pinia store (`customizer`) + matchMedia 实时同步
- 代码编辑器：Monaco Editor 用于配置文件编辑（darkprimary / darksecondary 单独维护）

### 鉴权 UX

- 从 `setupHttpClient()` 看是 token-based
- `confirmPlugin` 自研全局确认弹窗插件
- 登录页在路由中（`src/routes/` 下应有 auth 页）

### 实时更新

- 引入 `event-source-polyfill` ——支持 **Server-Sent Events (SSE)**
- axios + 拦截器处理认证
- 同时支持轮询 / SSE 两种模式（具体实现需读源码）

### 截图（来自 AstrBot README）

- 主截图（WebP）：在 README 中嵌入，URL 含 GitHub JWT 签名，会过期，需从 README 直接看
- 文档站：<https://docs.astrbot.app/> 有更完整截图

### 可借鉴要点

1. **用 CodedThemes Berry 起手**：免费 Vue admin 模板，含完整布局（侧边栏 / 顶栏 / 面包屑 / 设置抽屉）。但 yomua-bot 已经决定走 React + shadcn，参考价值有限。
2. **Monaco Editor 编辑配置文件**：比 textarea 体验好太多，但 MaiBot 用的是 CodeMirror（`[data-dashboard-code-editor='true']`）。两者都可考虑。
3. **OpenAPI 自动生成前端客户端**（`openapi-ts`）——yomua-bot 已有 webui.toml + Core 协议，可考虑生成 TypeScript 客户端避免手写类型。
4. **SSE 替代 WebSocket**：单向上报场景（日志、状态）比 WebSocket 更轻，浏览器原生 API。但双向场景（聊天）仍需 WebSocket。
5. **自定义 primary / secondary 色**：用户层面可调；与用户原计划"teal accent 固定"不同——更灵活的取舍。

### 与用户计划栈的差异

- AstrBot 选 **Vue + Vuetify + Material Design**；yomua-bot 计划 React + shadcn + Tailwind
- AstrBot 默认紫色调；yomua-bot 计划 teal
- AstrBot 用 Monaco + 自定义代码编辑；MaiBot 用 CodeMirror

---

## 4. Koishi

### 仓库与文档

- 主仓库：<https://github.com/koishijs/koishi>（6.2k★）
- 官网：<https://koishi.chat/>（中文友好）
- Console 文档：<https://koishi.chat/manual/console/>（存在但 webfetch 拿不到详情；首页加载了多张 console 截图）

### 关键定位

> **Koishi 是一个跨平台、可扩展、高性能的聊天机器人框架。**  
> "提供了高度便利的控制台，让你无需基础让你在几分钟之内搭建自己的聊天机器人"——来自 README

### 视觉与布局（基于首页截图）

首页底部列出了 5 个核心 console 截图，每张都提供 light / dark 两个版本：

| 页面 | 截图 URL（light） | 截图 URL（dark） |
|---|---|---|
| 主页（home） | `https://koishi.chat/manual/console/home.light.webp` | `https://koishi.chat/manual/console/home.dark.webp` |
| 设置（settings） | `https://koishi.chat/manual/console/settings.light.webp` | `https://koishi.chat/manual/console/settings.dark.webp` |
| 插件市场（market） | `https://koishi.chat/manual/console/market.light.webp` | `https://koishi.chat/manual/console/market.dark.webp` |
| 数据库（database） | `https://koishi.chat/manual/console/database.light.webp` | `https://koishi.chat/manual/console/database.dark.webp` |
| 沙盒（sandbox） | `https://koishi.chat/manual/console/sandbox.light.webp` | `https://koishi.chat/manual/console/sandbox.dark.webp` |

### 主要功能模块（来自 README 描述）

- **仪表盘**：实时监控机器人运行状态
- **插件配置**：可视化编辑
- **插件市场**：浏览 / 安装 / 更新插件
- **数据库**：可视化数据浏览与查询
- **沙盒**：模拟聊天、预览效果（"安装或配置任何插件后，立即在沙盒界面中模拟聊天"）
- **模块热重载**：保存即热更，无需重启

### 视觉风格

- Koishi 标志是古明地恋（橙发 + 帽子），整体品牌色以橙红为主
- 风格偏传统后台，浅色为主，左侧导航 + 右侧内容
- 支持 light / dark 双主题

### 可借鉴要点

1. **沙盒（sandbox）概念**——直接在 WebUI 中模拟聊天，对调试 plugin / 角色回复极有用。yomua-bot 也可以做"WebUI 内测试对话"，不必每次都通过 QQ 触发。
2. **模块热重载**——保存配置或插件无需重启，对开发体验是质的提升。
3. **截图命名约定**（`home.light.webp` / `home.dark.webp`）——文档化双主题的好实践。
4. **每张截图配 light/dark 两版**——主题切换营销效果更好。

### 不足

- Koishi 文档站使用 VitePress；其 Console 是 Koishi 框架内置，不是独立仓库，本次没拿到 console 源码细节。

---

## 5. NoneBot2

### 仓库

- 主仓库：<https://github.com/nonebot/nonebot2>（7.7k★）
- 框架本身：**不内置 WebUI**
- NoneBot-Desktop：是社区/官方的独立桌面工具，与 NoneBot2 主仓库分离
  - 注意：`https://github.com/nonebot/nonebot-desktop` 已 404
  - 当前实际位置未在本次调研内确认（需进一步搜索）

### NoneBot2 自带的 Web 管理能力

- **adapter-console**：终端交互适配器（不是 WebUI）
- 主框架代码不含 dashboard / webui 目录
- 设计哲学：NoneBot2 只负责协议与事件分发，管理 UI 由插件生态提供

### 视觉风格

- NoneBot2 本身没有 WebUI 视觉风格可借鉴
- NoneBot-Desktop（Electron 应用）若要看需另查

### 可借鉴要点

1. **协议与 UI 解耦**——NoneBot2 完全不绑死任何 WebUI 形式，yomua-bot 当前 `webui` crate 作为独立进程符合同样的设计原则。
2. **管理功能由插件 / 扩展提供**——而非核心内置。yomua-bot 也应避免把 WebUI 强依赖到 Core。

---

## 6. 跨项目共性总结

### 布局模式

- **侧边栏 + 顶栏 + 主内容区**：所有有 WebUI 的项目（NapCat / SnowLuma / MaiBot / AstrBot / Koishi）都采用此布局
  - 侧边栏宽 13–16rem（展开）/ 3–4rem（折叠）
  - 顶栏高 3–4rem
  - MaiBot 明确把布局尺寸做成 CSS 变量（`--layout-sidebar-width: 13rem` 等）
- 单页应用（SPA），客户端路由（TanStack Router / vue-router）

### 视觉风格

| 项目 | 调性 | 主色 | 主题切换 |
|---|---|---|---|
| NapCat | 浅紫 + 白 | 紫 | light / dark |
| MaiBot | shadcn 默认 + 双主题 | 橙红（modern） / 墨绿铁锈（future-retro） | light / dark / system |
| AstrBot | Material Design | 紫（可自定义） | light / dark / system |
| Koishi | 传统后台 | 橙红（品牌色） | light / dark |

→ **用户原计划"dark-only + teal accent"偏小众**。同类项目要么支持 light/dark，要么用品牌色而非 teal。

### 关键面板（出现频次 ≥ 3 个项目）

| 面板 | NapCat | MaiBot | AstrBot | Koishi |
|---|:-:|:-:|:-:|:-:|
| 仪表盘 / 状态总览 | ✓ | ✓ | ✓ | ✓ |
| 日志（实时） | ✓ | ✓ | ✓ | ✓ |
| 插件 / 扩展管理 | ✓ | ✓ | ✓ | ✓ |
| 配置管理 | ✓ | ✓ | ✓ | ✓ |
| 模型 / 服务配置 | – | ✓ | ✓ | – |
| 数据库浏览 | – | – | ✓ | ✓ |
| 聊天 / 沙盒测试 | – | ✓ | ✓ | ✓ |
| 角色 / 用户管理 | – | ✓（人物关系） | ✓ | – |
| 资源管理（表情 / 文件） | – | ✓ | – | – |
| 知识图谱 | – | ✓（ReactFlow） | – | – |
| 系统设置（含主题） | – | ✓ | ✓ | ✓ |

### 鉴权 UX

- **统一方案：Token / 密码**——MaiBot 是 token（自定义或自动生成）；SnowLuma 是启动日志里的初始密码；AstrBot 是 token + 自动生成
- 都有专门的登录页（独立路由，不是 Modal）
- Token 持久化在 localStorage / Pinia / Jotai
- 统一封装 fetch 拦截器（`fetch-with-auth.ts` 等）

### 实时更新

| 机制 | 项目 | 适用场景 |
|---|---|---|
| **WebSocket** | MaiBot（日志 + 聊天 + 进度） / SnowLuma / NapCat | 双向 / 频繁 / 高吞吐 |
| **SSE** | AstrBot | 单向上报（事件流） |
| **轮询** | NoneBot 生态插件常用 | 简单 / 低频 |

MaiBot 的选择最有代表性：高频双向用 WS，普通 API 用 TanStack Query 缓存。

### 状态管理

| 项目 | 选择 |
|---|---|
| MaiBot | Jotai（原子化） + TanStack Query（服务器状态） |
| AstrBot | Pinia（Vue 经典） |
| Koishi | 自研 store |
| NapCat / SnowLuma | 未深入（Vue 生态常见 Pinia / Vuex） |

→ **Jotai + TanStack Query 是 React 生态当下最清晰的分层方案**。

---

## 7. 可借鉴要点 / Recommendations for yomua-bot

> 用户原计划栈：React + shadcn/ui + Tailwind、dark-only、teal accent。  
> 以下建议优先考虑是否调整。

### 强烈建议（P0）

1. **沿用 shadcn/ui + Radix + Tailwind v4 + 设计 token 化**：与 MaiBot 同源，是当前 React 生态最成熟、可控性最高的方案。把 MaiBot CSS 里的 `--color-primary`、`--layout-sidebar-width`、`--visual-radius-*` 这套 token 命名直接借鉴过来。

2. **统一鉴权 = Token + 启动日志初始值**：参考 SnowLuma 的"启动日志打印初始 token"模式，配合 MaiBot 的"自定义 / 自动生成"切换。Token 持久化在 localStorage + Jotai atom。

3. **实时更新分层**：高频双向（聊天测试 / 日志流）→ WebSocket；状态/统计 → TanStack Query 自动轮询；单向事件流（可选）→ SSE。避免所有场景都用 WS（AstrBot 的 SSE 思路也值得参考）。

4. **配置面板做成自动生成表单**：MaiBot 的"自动解析 dataclass → 自动生成表单 → 防抖自动保存 → 一键重启生效"是杀手级特性。yomua-bot 的 `runtime.toml` / `webui.toml` / `llm.toml` 完全可以走同一条路。后端把 TOML schema 暴露成 JSON Schema，前端用 react-hook-form + zod 渲染。

### 建议采纳（P1）

5. **布局结构 = sidebar (13rem) + header (3.5rem) + main**，与 MaiBot 数值一致；支持 sidebar 折叠到 4rem。所有尺寸用 CSS 变量集中管理，便于以后调。

6. **核心面板清单（按本项目 Phase 9 范围裁剪）**：
   - 仪表盘：Core 连接状态、绑定数、运行时间、近期事件流
   - 角色管理：角色列表 + 状态条（精力/注意力/压力）+ 详情编辑
   - 会话绑定：表格 + 过滤 + 切换角色
   - 配置管理：runtime.toml / webui.toml / llm.toml 三页
   - 日志：WebSocket + 虚拟滚动 + 多级过滤
   - 插件管理：列表 + 启停 + 重载
   - 系统设置：主题（dark-only 但保留 system 选项）、token 管理

7. **从 shadcn/ui `components.json` 起步**：参考 MaiBot 的 `components.json`，明确 `style: "new-york"`、`rsc: false`、`baseColor: "slate"`。然后在 `index.css` 里覆盖 `--primary` 到 teal。

### 可选（P2）

8. **不一定要 dark-only**：考虑 light / dark / system 三态。MaiBot / AstrBot 都是三态。Dark-only 限制会与 macOS 用户的系统主题期望冲突（白天电脑主题切到浅色时打开后台会很刺眼）。即使保留 dark-only，**至少要尊重系统 prefers-color-scheme**——MaiBot 的 `system` 模式实现很简单，可以直接抄。

9. **本地测试对话（沙盒）**：参考 Koishi 的 sandbox 概念，在 WebUI 中提供"测试对话"页——直接调 Core 的对话管线而不走 QQ。极大降低角色调试成本。

10. **截图 / 设计 token 命名约定**：参考 Koishi 的 `home.light.webp` / `home.dark.webp` 双版本截图规范，对未来文档化有帮助。

### 视觉风格的具体建议

| 维度 | 当前计划 | 调整建议 | 理由 |
|---|---|---|---|
| 主色 | teal accent | teal accent（HCT：~`hsl(180 70% 45%)`）保持 | MaiBot 主色橙红在情感场景里过暖；teal 在角色 Bot 语境更"科技+克制" |
| 主题 | dark-only | dark + system 自动 | 改造成本极低，体验提升明显 |
| 圆角 | 假设 0.5rem 默认 | 沿用 shadcn 默认（`--radius: 0.5rem`） | 与 shadcn 组件默认一致，少量自定义 |
| 字体 | 未定 | UI 用系统字体栈；代码用 JetBrains Mono | 与 MaiBot 一致，跨平台零字体加载成本 |

### 与原计划的显著分歧

- **dark-only → system 自动**：唯一值得在动手前再确认的决策点。
- **teal accent 可保留**：没有项目用 teal 做主色，反而是差异化机会。
- **shadcn/ui + Tailwind v4 选择正确**：与 MaiBot 同步，可直接借鉴其 Provider 嵌套、CSS Token 命名、Panel 分层。
- **WebUI 必须独立进程（已是当前设计）**：与 SnowLuma / NoneBot2 哲学一致，无需调整。

---

## 8. 引用与原始资料

### 仓库与文档

- NapCatQQ：<https://github.com/NapNeko/NapCatQQ>
- SnowLuma：<https://github.com/SnowLuma/SnowLuma>
- NapCat WebUI packages：<https://github.com/NapNeko/NapCatQQ/tree/main/packages/napcat-webui-frontend>、<https://github.com/NapNeko/NapCatQQ/tree/main/packages/napcat-webui-backend>
- MaiBot：<https://github.com/Mai-with-u/MaiBot>
- MaiBot Dashboard README：<https://github.com/Mai-with-u/MaiBot/tree/main/dashboard>
- MaiBot Dashboard `index.html`：<https://github.com/Mai-with-u/MaiBot/blob/main/dashboard/index.html>
- MaiBot Dashboard `main.tsx`：<https://github.com/Mai-with-u/MaiBot/blob/main/dashboard/src/main.tsx>
- MaiBot Dashboard CSS tokens：<https://github.com/Mai-with-u/MaiBot/blob/main/dashboard/src/index.css>
- AstrBot：<https://github.com/AstrBotDevs/AstrBot>
- AstrBot Dashboard README：<https://github.com/AstrBotDevs/AstrBot/tree/master/dashboard>
- AstrBot Dashboard `package.json`：<https://github.com/AstrBotDevs/AstrBot/blob/master/dashboard/package.json>
- AstrBot Dashboard `main.ts`：<https://github.com/AstrBotDevs/AstrBot/blob/master/dashboard/src/main.ts>
- AstrBot 文档站：<https://docs.astrbot.app/>
- Koishi：<https://github.com/koishijs/koishi>
- Koishi 官网：<https://koishi.chat/>
- NoneBot2：<https://github.com/nonebot/nonebot2>

### 截图

- MaiBot 主界面：<https://github.com/Mai-with-u/MaiBot/raw/main/dashboard/docs/main.png>
- MaiBot WebUI 总览：<https://github.com/Mai-with-u/MaiBot/raw/main/depends-data/webui-showcase.jpg>
- Koishi Console 系列截图（light/dark 各 5 张）：
  - home：<https://koishi.chat/manual/console/home.light.webp>、<https://koishi.chat/manual/console/home.dark.webp>
  - settings：<https://koishi.chat/manual/console/settings.light.webp>、<https://koishi.chat/manual/console/settings.dark.webp>
  - market：<https://koishi.chat/manual/console/market.light.webp>、<https://koishi.chat/manual/console/market.dark.webp>
  - database：<https://koishi.chat/manual/console/database.light.webp>、<https://koishi.chat/manual/console/database.dark.webp>
  - sandbox：<https://koishi.chat/manual/console/sandbox.light.webp>、<https://koishi.chat/manual/console/sandbox.dark.webp>
- SnowLuma Hero 图：<https://github.com/SnowLuma/SnowLuma/blob/main/assets/readme/hero.svg>

### 本调研的局限

- **NapCat WebUI 源码未深入读**：只确认存在 `napcat-webui-frontend` / `napcat-webui-backend` 两个包，未读具体面板划分。
- **Koishi console 源码未拿到**：`<https://koishi.chat/manual/console/>` URL 在 webfetch 中持续 404，仅从首页截图与 README 描述推断。
- **NoneBot-Desktop 实际位置未确认**：`https://github.com/nonebot/nonebot-desktop` 已 404，需另行搜索。
- AstrBot README 中嵌入的 WebP 截图 URL 含 GitHub JWT 签名（短期 token），会过期——建议直接访问项目 README 获取最新截图。
