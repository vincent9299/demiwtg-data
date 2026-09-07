# webgate/searxng 上游来源

- 来源：https://github.com/searxng/searxng.git
- 入仓 pin：15a91992e474c697ec2b0227865e85d200ec7dd6（2026-09-04，浅克隆后去 .git 入仓）
- 本目录现为项目自有代码：中文源引擎（toutiao/baidu/huaban/pixiv）、
  wikicommons 授权扩展等直接在此维护，随本仓版本化。
- 拉上游更新（按需，先跑冒烟再部署）：
  git remote add searxng-upstream https://github.com/searxng/searxng.git  # 一次性
  git fetch searxng-upstream && git merge searxng-upstream/master -X ours  # 冲突保我们的
- 反哺社区：单独 fork + cherry-pick 相关引擎后向上游提 PR，勿混入本仓日常提交。
