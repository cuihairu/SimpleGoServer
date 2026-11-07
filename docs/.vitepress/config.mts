import { withMermaid } from "vitepress-plugin-mermaid";
import { defineConfig } from "vitepress";

const repository = process.env.GITHUB_REPOSITORY ?? "";
const repositoryName = repository.split("/")[1] ?? "";
const isUserOrOrgPagesRepo = repositoryName.endsWith(".github.io");
const base =
  process.env.GITHUB_ACTIONS === "true" && repositoryName && !isUserOrOrgPagesRepo
    ? `/${repositoryName}/`
    : "/";

const enTheme = {
  siteTitle: "SimpleGoServer",
  logo: "/logo.svg",
  nav: [
    { text: "Design", link: "/design" },
    { text: "Architecture", link: "/architecture" },
    { text: "Glossary", link: "/glossary" },
    { text: "Code", link: "https://github.com/cuihairu/SimpleGoServer/tree/main/http-service" },
  ],
  sidebar: {
    "/": [
      {
        text: "http-service",
        items: [
          { text: "Design trade-offs", link: "/design" },
          { text: "Architecture and layers", link: "/architecture" },
          { text: "Glossary", link: "/glossary" },
          { text: "Service boundary", link: "/design#service-boundary" },
          { text: "Layers and dependencies", link: "/design#layers-and-dependency-direction" },
          {
            text: "Decision tables",
            collapsed: false,
            items: [
              { text: "Lifecycle", link: "/design#lifecycle" },
              { text: "HTTP layer", link: "/design#http-layer" },
              { text: "Business and storage", link: "/design#business-and-storage" },
              { text: "Testability", link: "/design#testability" },
            ],
          },
          { text: "Code walkthroughs", link: "/design#code-walkthroughs" },
          { text: "Testing in practice", link: "/design#testing-in-practice" },
          { text: "What is not here", link: "/design#what-is-not-here" },
          { text: "Reproduce", link: "/design#reproduce" },
        ],
      },
    ],
  },
  socialLinks: [
    { icon: "github", link: "https://github.com/cuihairu/SimpleGoServer" },
  ],
  footer: {
    message: "Released under the MIT License",
    copyright: "Copyright © 2024-present cuihairu",
  },
  outline: { level: [2, 3], label: "On this page" },
  search: { provider: "local" },
  docFooter: { prev: "Previous", next: "Next" },
  lastUpdated: { text: "Last updated" },
};

const zhTheme = {
  ...enTheme,
  siteTitle: "SimpleGoServer",
  nav: [
    { text: "设计取舍", link: "/zh/design" },
    { text: "架构分层", link: "/zh/architecture" },
    { text: "术语表", link: "/zh/glossary" },
    { text: "代码", link: "https://github.com/cuihairu/SimpleGoServer/tree/main/http-service" },
  ],
  sidebar: {
    "/zh/": [
      {
        text: "http-service",
        items: [
          { text: "设计取舍", link: "/zh/design" },
          { text: "架构分层", link: "/zh/architecture" },
          { text: "术语表", link: "/zh/glossary" },
          { text: "服务边界", link: "/zh/design#服务边界" },
          { text: "分层与依赖方向", link: "/zh/design#分层与依赖方向" },
          {
            text: "决策表",
            collapsed: false,
            items: [
              { text: "生命周期", link: "/zh/design#生命周期" },
              { text: "HTTP 层", link: "/zh/design#http-层" },
              { text: "业务与存储", link: "/zh/design#业务与存储" },
              { text: "可测试性", link: "/zh/design#可测试性" },
            ],
          },
          { text: "代码走查", link: "/zh/design#代码走查" },
          { text: "测试怎么落地", link: "/zh/design#测试怎么落地" },
          { text: "没做的事", link: "/zh/design#没做的事" },
          { text: "复现", link: "/zh/design#复现" },
        ],
      },
    ],
  },
  footer: {
    message: "基于 MIT 许可发布",
    copyright: "Copyright © 2024-present cuihairu",
  },
  outline: { level: [2, 3], label: "页面导航" },
  docFooter: { prev: "上一页", next: "下一页" },
  lastUpdated: { text: "最后更新于" },
};

export default withMermaid(defineConfig({
  lang: "en-US",
  title: "SimpleGoServer",
  titleTemplate: false,
  description:
    "Design trade-offs in a standard-library Go HTTP service: lifecycle, timeout matrix, concurrency choice, error mapping, and what is deliberately left out.",
  base,
  cleanUrls: true,
  lastUpdated: true,
  outDir: ".vitepress/dist",
  head: [
    ["link", { rel: "icon", type: "image/svg+xml", href: `${base}favicon.svg` }],
    ["link", { rel: "preconnect", href: "https://fonts.googleapis.com" }],
    ["link", { rel: "preconnect", href: "https://fonts.gstatic.com", crossorigin: "" }],
    [
      "link",
      {
        rel: "stylesheet",
        href: "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Geist+Mono:wght@400;500&display=swap",
      },
    ],
    ["meta", { name: "theme-color", content: "#00add8" }],
    ["meta", { property: "og:type", content: "website" }],
    ["meta", { property: "og:locale", content: "en_US" }],
    ["meta", { property: "og:title", content: "SimpleGoServer" }],
    [
      "meta",
      {
        property: "og:description",
        content:
          "Design trade-offs in a Go service: every key choice records the alternative and why it was rejected.",
      },
    ],
  ],
  locales: {
    root: {
      label: "English",
      lang: "en-US",
      title: "SimpleGoServer",
      description:
        "Design trade-offs in a standard-library Go HTTP service: lifecycle, timeouts, concurrency, error mapping, and explicit non-goals.",
      themeConfig: enTheme,
    },
    zh: {
      label: "简体中文",
      lang: "zh-CN",
      link: "/zh/",
      title: "SimpleGoServer",
      description:
        "标准库写的内存键值 HTTP 服务的设计取舍：生命周期、超时矩阵、并发选型、错误映射，以及明确没做的事与生产环境的补法。",
      themeConfig: zhTheme,
    },
  },
}))
