import { defineConfig } from "vitepress";

// GitHub Pages 项目站挂在 /simplegoserver/ 下：CI 里从 GITHUB_REPOSITORY 推 base，
// 本地 dev/preview 用 "/"，两边一套配置。（同款推导见 JsonStream 的站点。）
const repository = process.env.GITHUB_REPOSITORY ?? "";
const repositoryName = repository.split("/")[1] ?? "";
const isUserOrOrgPagesRepo = repositoryName.endsWith(".github.io");
const base =
  process.env.GITHUB_ACTIONS === "true" && repositoryName && !isUserOrOrgPagesRepo
    ? `/${repositoryName.toLowerCase()}/`
    : "/";

export default defineConfig({
  lang: "zh-CN",
  title: "SimpleGoServer",
  titleTemplate: false,
  description:
    "两道后端面试题的设计文档站——主线是「为什么这样设计」：主从 Reactor 与自定义 TCP 帧协议（重题）、标准库 HTTP 服务的工程形态（轻题），每个关键选择都给出备选方案与放弃理由。",
  base,
  cleanUrls: true,
  lastUpdated: true,
  outDir: ".vitepress/dist",
  themeConfig: {
    siteTitle: "SimpleGoServer",
    logo: "/logo.svg",
    nav: [
      { text: "题一 · Reactor 与自定义协议", link: "/q1-design" },
      { text: "题二 · http-service", link: "/q2-design" },
      {
        text: "深入资料",
        items: [
          { text: "架构与取舍（DESIGN）", link: "/DESIGN" },
          { text: "自定义协议说明（Proto）", link: "/Proto" },
          { text: "需求分析（Analysis）", link: "/Analysis" },
          { text: "性能基准（Benchmark）", link: "/Benchmark" },
          { text: "知识点梳理（NOTES）", link: "/NOTES" },
        ],
      },
    ],
    sidebar: {
      "/": [
        {
          text: "设计主线：为什么这样设计",
          items: [
            { text: "总览：两题一套方法论", link: "/" },
            {
              text: "题一：Reactor 与自定义协议",
              link: "/q1-design",
            },
            { text: "题二：http-service", link: "/q2-design" },
          ],
        },
        {
          text: "题目一深入",
          items: [
            { text: "架构与取舍（DESIGN）", link: "/DESIGN" },
            { text: "自定义协议说明（Proto）", link: "/Proto" },
            { text: "需求分析（Analysis）", link: "/Analysis" },
            { text: "性能基准（Benchmark）", link: "/Benchmark" },
          ],
        },
        {
          text: "复习与工程化",
          items: [{ text: "知识点梳理（NOTES）", link: "/NOTES" }],
        },
      ],
    },
    socialLinks: [
      { icon: "github", link: "https://github.com/cuihairu/SimpleGoServer" },
    ],
    footer: {
      message: "基于 MIT 许可发布",
      copyright: "Copyright © 2024-present cuihairu",
    },
    outline: {
      level: [2, 3],
      label: "页面导航",
    },
    search: {
      provider: "local",
    },
    docFooter: {
      prev: "上一页",
      next: "下一页",
    },
    lastUpdated: {
      text: "最后更新于",
    },
  },
  head: [
    ["link", { rel: "icon", type: "image/svg+xml", href: `${base}favicon.svg` }],
    ["meta", { name: "theme-color", content: "#00add8" }],
    ["meta", { property: "og:type", content: "website" }],
    ["meta", { property: "og:locale", content: "zh-CN" }],
    ["meta", { property: "og:title", content: "SimpleGoServer" }],
    [
      "meta",
      {
        property: "og:description",
        content:
          "两道后端面试题的设计文档站——主线是「为什么这样设计」：每个关键选择都给出备选方案与放弃理由。",
      },
    ],
  ],
});
