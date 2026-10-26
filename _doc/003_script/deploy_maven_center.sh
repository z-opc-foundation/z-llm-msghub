#!/usr/bin/env bash
#
# deploy_maven_center.sh — z-llm-agent 一键发布到 Maven Central
#
# 子命令：
#   publish    实际 mvn deploy -Pcentral（默认）
#   verify     验证 Central 上 1.0.47 是否可查
#   help       显示此帮助
#
# 与 z-util 同款的机制（凭证 .env / 仓内 .gnupg keyring / gpg-with-passphrase.sh），
# GPG 私钥与 z-util 同一把（2026-10-26 从 z-util/.gnupg 拷贝，指纹
# DCD08D7333937E5D5678EA73720B6C5DC394BF25），无需再跑 gpg-init。
#
set -eo pipefail

chmod +x "$0" 2>/dev/null || true

log()  { printf "[deploy] %s\n" "$*"; }
die()  { printf "[deploy][ERROR] %s\n" "$*" >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"
[[ -f pom.xml ]] || die "请在 z-llm-agent 仓库根目录运行此脚本"

cmd_publish() {
    [[ -f .env ]] || die ".env 不存在（CENTRAL_USERNAME / CENTRAL_TOKEN / CENTRAL_GPG_PASSPHRASE）"
    set -a; source .env; set +a
    [[ -n "${CENTRAL_USERNAME:-}" ]] || die ".env 缺 CENTRAL_USERNAME"
    [[ -n "${CENTRAL_TOKEN:-}" ]]    || die ".env 缺 CENTRAL_TOKEN"
    [[ -n "${CENTRAL_GPG_PASSPHRASE:-}" ]] || die ".env 缺 CENTRAL_GPG_PASSPHRASE"
    [[ -d ./.gnupg ]] || die "./.gnupg 不存在（GPG keyring 应随仓目录本地存放，不入 git）"

    export GNUPGHOME="$PWD/.gnupg"

    log "═══════════════════════════════════════════════════════════════"
    log " 即将把 z-llm-agent（5 模块 reactor）上传到 Maven Central"
    log "  groupId : io.github.yuku123"
    log "  version : $(grep '<revision>' pom.xml | sed 's/.*<revision>\(.*\)<\/revision>.*/\1/')"
    log "  GPG KEY : $(GNUPGHOME="$GNUPGHOME" gpg --list-secret-keys --keyid-format short 2>/dev/null | grep -oE '[A-F0-9]{8}$' | head -1)"
    log "═══════════════════════════════════════════════════════════════"

    # 清掉 staging 避免上次残留
    rm -rf target/central-staging target/central-publishing target/central-deferred 2>/dev/null

    mvn -B deploy \
        -Pcentral \
        -DskipTests \
        -Dgpg.skip=false \
        2>&1 | tee /tmp/z-llm-agent-deploy.log | tail -60

    if grep -q "BUILD SUCCESS" /tmp/z-llm-agent-deploy.log \
       && grep -q "Uploaded bundle successfully" /tmp/z-llm-agent-deploy.log; then
        log "✅ BUILD SUCCESS + Bundle uploaded"
        log "Central Portal 控制台：https://central.sonatype.com/publishing/deployments"
        return 0
    fi

    # ── 兜底：本机代理（127.0.0.1:57681）会让 central-publishing-maven-plugin 的
    #    HTTP 客户端在上传阶段 Broken pipe（1.0.47 实测 4 连败，bundle 963K 也断，
    #    与体积无关）。构建/签名/staging 都是好的，只是上传这一步过不去。
    #    解法：bundle 已生成，用 curl 直传 Portal API（同一条链路 curl 就通，3 秒 201）。
    if [ -f target/central-publishing/central-bundle.zip ] && grep -q "BUILD SUCCESS" /tmp/z-llm-agent-deploy.log; then
        log "mvn 上传被本机代理掐断，改用 curl 直传 Portal API…"
        RESP=$(curl -s --max-time 120 \
            -u "$CENTRAL_USERNAME:$CENTRAL_TOKEN" \
            -X POST "https://central.sonatype.com/api/v1/publisher/upload?publishingType=AUTOMATIC" \
            -F "bundle=@target/central-publishing/central-bundle.zip" \
            -w "\nHTTP_CODE:%{http_code}")
        CODE=$(echo "$RESP" | grep -oE "HTTP_CODE:[0-9]+" | cut -d: -f2)
        BODY=$(echo "$RESP" | sed 's/HTTP_CODE:[0-9]*//')
        log "Portal 响应: HTTP $CODE, deploymentId=$BODY"
        if [ "$CODE" = "201" ]; then
            log "✅ 上传成功（AUTOMATIC，Portal 自动校验并发布）"
            log "  部署 ID: $BODY"
            log "  状态查询: curl -u \$CENTRAL_USERNAME:\$CENTRAL_TOKEN \\"
            log "    'https://central.sonatype.com/api/v1/publisher/status?id=$BODY'"
            log "  5~15 分钟后 repo1 可查：bash _doc/003_script/deploy_maven_center.sh verify"
            return 0
        fi
        die "curl 直传也失败（HTTP $CODE）。看响应体：$BODY"
    fi

    die "BUILD FAILURE，请看 /tmp/z-llm-agent-deploy.log"
}

cmd_verify() {
    V=$(grep '<revision>' pom.xml | sed 's/.*<revision>\(.*\)<\/revision>.*/\1/')
    for A in z-llm-agent z-llm-agent-common z-llm-agent-proxy z-llm-agent-core z-llm-agent-client z-llm-agent-starter; do
        printf "%-22s " "$A:$V"
        curl -s -o /dev/null -w "%{http_code}\n" --max-time 15 \
            "https://repo1.maven.org/maven2/io/github/yuku123/$A/$V/$A-$V.pom"
    done
    log "（200=已同步；404=还没到，Portal 校验一般 5~15 分钟）"
}

case "${1:-publish}" in
    publish) cmd_publish ;;
    verify)  cmd_verify ;;
    help|-h|--help) sed -n '2,16p' "$0" ;;
    *) die "未知子命令：$1（用 help 看用法）" ;;
esac
