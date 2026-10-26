#!/bin/bash
#
# z-llm-msghub 生产环境部署脚本（参数化模板）
# 通过自建发布平台触发 prod 环境构建+部署，地址与凭据通过环境变量注入：
#
#   MATRIX_BASE   发布平台基址（默认 http://localhost）
#   MATRIX_COOKIE 发布平台登录 Cookie（必填，否则接口返回未登录）
#
# 用法: MATRIX_BASE=https://deploy.example.com MATRIX_COOKIE='...' ./deploy-prod.sh
#

MATRIX_BASE="${MATRIX_BASE:-http://localhost}"
MATRIX_COOKIE="${MATRIX_COOKIE:-}"

if [ -z "$MATRIX_COOKIE" ]; then
    echo "缺少 MATRIX_COOKIE（发布平台登录凭据），请通过环境变量注入" >&2
    exit 1
fi

curl "${MATRIX_BASE}/v2/releaseManage/deploy/create/master" \
  -H 'Accept: application/json' \
  -H 'Content-Type: application/json; charset=UTF-8' \
  -b "${MATRIX_COOKIE}" \
  -H "Origin: ${MATRIX_BASE}" \
  -H "Referer: ${MATRIX_BASE}/matrix/application/detail/appDeploy?activeTab=prod&appCode=z-llm-msghub" \
  --data-raw '{"masterBranch":"master","envCodes":["base-prod"],"deployModel":"online","pipelineCode":"z-llm-msghub_prod_defaultPipeline","buildType":"beServerBuild","pdaDeployType":""}' \
  --insecure
