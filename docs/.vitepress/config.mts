import { defineConfig } from "vitepress";

// GitHub Pages 项目站挂在 /SimpleGoServer/ 下：CI 里从 GITHUB_REPOSITORY 推 base，
// 本地 dev/preview 用 "/"，两边一套配置。（推导同 JsonStream 的站点，但**不**
// toLowerCase——Pages 项目站的 URL 大小写敏感，必须用仓库名原样：
// /simplegoserver/ 实测 "Site not found"，只有 /SimpleGoServer/ 可达。）
const repository = process.env.GITHUB_REPOSITORY ?? "";
const repositoryName = repository.split("/")[1] ?? "";
const isUserOrOrgPagesRepo = repositoryName.endsWith(".github.io");
const base =
  process.env.GITHUB_ACTIONS === "true" && repositoryName && !isUserOrOrgPagesRepo
    ? `/${repositoryName}/`
    : "/";

export default defineConfig({
  lang: "zh-CN",
  title: "SimpleGoServer",
  titleTemplate: false,
  description:
    "标准库写的内存键值 HTTP 服务的设计取舍：生命周期、超时矩阵、并发选型、错误映射，以及明确没做的事与生产环境的补法。",
  base,
  cleanUrls: true,
  lastUpdated: true,
  outDir: ".vitepress/dist",
  themeConfig: {
    siteTitle: "SimpleGoServer",
    logo: "/logo.svg",
    nav: [
      { text: "设计取舍", link: "/design" },
      { text: "代码", link: "https://github.com/cuihairu/SimpleGoServer/tree/main/http-service" },
    ],
    sidebar: {
      "/": [
        {
          text: "http-service",
          items: [
            { text: "设计取舍", link: "/design" },
            { text: "服务边界", link: "/design#服务边界" },
            { text: "分层与依赖方向", link: "/design#分层与依赖方向" },
            {
              text: "决策表",
              collapsed: false,
              items: [
                { text: "生命周期", link: "/design#生命周期" },
                { text: "HTTP 层", link: "/design#http-层" },
                { text: "业务与存储", link: "/design#业务与存储" },
                { text: "可测试性", link: "/design#可测试性" },
              ],
            },
            { text: "代码走查", link: "/design#代码走查" },
            { text: "测试怎么落地", link: "/design#测试怎么落地" },
            { text: "没做的事", link: "/design#没做的事" },
            { text: "复现", link: "/design#复现" },
          ],
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
